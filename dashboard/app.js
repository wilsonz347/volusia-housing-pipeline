/**
 * app.js — Volusia Housing Pipeline · Dashboard Logic
 *
 * Consumes two dbt mart CSVs served from /data/:
 *   mart_housing_pressure.csv   — one row per ZIP code (29 ZIPs)
 *   mart_permit_velocity.csv    — one row per council district (5 districts)
 *
 * All business logic lives in the dbt marts. This file is a read-only
 * presentation layer: it loads, types, and renders mart output — it does
 * not reimplement classification rules, re-aggregate raw CAMA data, or
 * duplicate any transformation that belongs in dbt.
 *
 * Module layout
 * ─────────────
 *  1. Constants & metadata
 *  2. Application state
 *  3. Boot sequence (Promise.all → typeRow → addPressureScores → render)
 *  4. Event binding
 *  5. Render orchestration (renderAll → individual render fns)
 *  6. Analytical helpers (weightedPct, correlation, median, topBy …)
 *  7. CSV utilities (loadCsv, parseCsv, typeRow)
 *  8. Format utilities (pct, money, rate, corrFmt …)
 *  9. Export
 *
 * Data integrity notes
 * ────────────────────
 *  • weightedPct() weights each ZIP's percentage by its residential_parcels
 *    count. This produces the true county-wide rate. Simple AVG or median
 *    understates investor concentration because large coastal ZIPs (32118,
 *    32169) carry far more parcels than inland ZIPs.
 *
 *  • parseCsv() is a minimal RFC-4180-aware parser. It handles quoted fields
 *    and embedded commas. Supabase exports do not currently produce commas
 *    inside numeric fields, but the parser is safe against future schema
 *    changes that add string columns (e.g. city name, classification label).
 *
 *  • addPressureScores() returns null for ZIPs that are missing all ACS
 *    fields (rent_to_income_ratio, renter_occupied_ratio, vacant_unit_ratio).
 *    Those ZIPs — 32102, 32129, 32136, 32754 — lack ACS data and would
 *    receive artificially low scores if missing values were treated as zero.
 *    They are excluded from pressure rankings but appear in all other views.
 *
 *  • The pressure score is a normalized composite, not a mart field. Weights:
 *      all_investor_pct          0.32
 *      rent_to_income_ratio      0.22
 *      renter_occupied_ratio     0.14
 *      median_soh_differential   0.12
 *      vacant_unit_ratio         0.10
 *      owner_occupied_pct (inv)  0.10  ← inverted: lower OO = higher pressure
 *    Scores are min-max normalized within the visible row set, so they shift
 *    when ZIP filters are applied. This is intentional: the score reflects
 *    relative pressure within the current view, not an absolute county index.
 */

// ─── 1. Constants & metadata ──────────────────────────────────────────────────

const HOUSING_PATH = "data/mart_housing_pressure.csv";
const PERMITS_PATH = "data/mart_permit_velocity.csv";

/**
 * metricMeta maps mart field names to display properties used by
 * renderRanking() and the rank metric selector. Adding a new rankable
 * field requires only a new entry here plus an <option> in the HTML.
 */
const metricMeta = {
  pressure_score:          { label: "Housing pressure score",       fmt: scoreFmt, color: "#b94a48" },
  all_investor_pct:        { label: "All investor %",               fmt: pct,      color: "#2364aa" },
  high_conf_investor_pct:  { label: "High-confidence investor %",   fmt: pct,      color: "#198f8a" },
  owner_occupied_pct:      { label: "Owner-occupied %",             fmt: pct,      color: "#367b48" },
  median_soh_differential: { label: "Median SOH differential",      fmt: money,    color: "#c77a16" },
  rent_to_income_ratio:    { label: "Rent-to-income ratio",         fmt: ratioPct, color: "#b94a48" },
  overdose_rate_2024:      { label: "2024 overdose rate",           fmt: rate,     color: "#7a4f9f" },
};

// ─── 2. Application state ─────────────────────────────────────────────────────

/**
 * state holds the typed mart rows after boot. Render functions read from
 * state; nothing outside the boot sequence writes to it.
 */
const state = {
  housing: [],   // mart_housing_pressure rows, augmented with pressure_score
  permits: [],   // mart_permit_velocity rows
};

// ─── 3. Boot sequence ─────────────────────────────────────────────────────────

/**
 * Load both mart CSVs in parallel, type every field, compute pressure scores,
 * bind UI events, and trigger the first render. Errors surface to the console;
 * a production deployment should wire these to a visible error banner.
 */
Promise.all([loadCsv(HOUSING_PATH), loadCsv(PERMITS_PATH)])
  .then(([housing, permits]) => {
    state.housing = addPressureScores(housing.map(typeRow));
    state.permits = permits.map(typeRow);
    bindEvents();
    renderAll();
  })
  .catch((err) => console.error("Failed to load mart data:", err));

// ─── 4. Event binding ─────────────────────────────────────────────────────────

function bindEvents() {
  // Tab navigation — toggles .active on both the button and the view section.
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      state.activeView = tab.dataset.view;
      document.querySelectorAll(".tab").forEach((el) => el.classList.toggle("active", el === tab));
      document.querySelectorAll(".view").forEach((el) => el.classList.toggle("active", el.id === state.activeView));
      renderAll();
    });
  });

  // Filter controls — any change re-renders the full dashboard with the
  // new visible row set. All filters are additive (AND logic).
  ["zipSearch", "metricSelect", "sortMode", "acsOnly"].forEach((id) => {
    document.getElementById(id).addEventListener("input", renderAll);
  });

  document.getElementById("exportSummary").addEventListener("click", exportSummary);
}

// ─── 5. Render orchestration ──────────────────────────────────────────────────

/**
 * renderAll is the single re-render entry point. Every filter change and tab
 * switch calls this. Individual render functions receive the filtered row set
 * so they stay pure — they do not read from state directly.
 */
function renderAll() {
  const rows = getVisibleRows();
  document.getElementById("zipCountPill").textContent = `Top 10`;
  renderAnswers(rows);
  renderKpis(rows);
  renderRanking("rankChart", rows, getMetric(), 10);
  renderMix(rows);
  renderInvestorChart(rows);
  renderComposition(rows);
  renderPressure(rows);
  renderDriverPanel(rows);
  renderZipTable(rows);
  renderSoh(rows);
  renderGaps(rows);
  renderScatter(rows);
  renderHealthList(rows);
  renderPermits();
}

/**
 * Returns the housing mart rows that pass all active filters.
 * Filters:
 *   zipSearch — substring match on zip_code (string comparison)
 *   acsOnly   — exclude ZIPs where has_acs_data is false
 */
function getVisibleRows() {
  const search  = document.getElementById("zipSearch").value.trim();
  const acsOnly = document.getElementById("acsOnly").checked;
  return state.housing.filter((row) => {
    const matchZip = !search || String(row.zip_code).includes(search);
    const matchAcs = !acsOnly || row.has_acs_data === true;
    return matchZip && matchAcs;
  });
}

function getMetric() {
  return document.getElementById("metricSelect").value;
}

/**
 * renderAnswers generates plain-English answers to the five core project
 * questions from mart data. Each answer is derived fresh from the visible
 * row set, so it responds to ZIP filters and the ACS-only toggle.
 */
function renderAnswers(rows) {
  const empty = "No ZIPs match the current filters. Clear the ZIP search or expand the filter set.";
  const answerIds = ["coreAnswer", "investorAnswer", "pressureAnswer", "sohAnswer", "healthAnswer", "permitAnswer"];

  if (!rows.length) {
    answerIds.forEach((id) => setText(id, empty));
    return;
  }

  const topInvestor  = topBy(rows, "all_investor_pct");
  const topHighConf  = topBy(rows, "high_conf_investor_pct");
  const topPressure  = topBy(rows, "pressure_score");
  const topSoh       = topBy(rows, "median_soh_differential");
  const topRent      = topBy(rows.filter((r) => Number.isFinite(r.rent_to_income_ratio)), "rent_to_income_ratio");
  const topOd        = topBy(rows.filter((r) => Number.isFinite(r.overdose_rate_2024)), "overdose_rate_2024");
  const topPermit    = topBy(state.permits, "total_permits");
  const corrInvOd    = correlation(rows, "all_investor_pct", "overdose_rate_2024");
  const corrRentOd   = correlation(rows, "rent_to_income_ratio", "overdose_rate_2024");

  setText("coreAnswer",
    `Current pressure is most visible in ZIP ${topPressure.zip_code}, where a ${scoreFmt(topPressure.pressure_score)} composite ` +
    `pressure score combines ${pct(topPressure.all_investor_pct)} all-investor ownership, ` +
    `${ratioPct(topPressure.rent_to_income_ratio)} rent-to-income, ` +
    `${pct(topPressure.renter_occupied_ratio * 100)} renter occupancy, and ` +
    `${money(topPressure.median_soh_differential)} median SOH protection. ` +
    `True acceleration still requires historical ownership rolls.`
  );
  setText("investorAnswer",
    `Investor ownership is concentrated most heavily in ZIP ${topInvestor.zip_code} at ${pct(topInvestor.all_investor_pct)} all-investor ownership; ` +
    `ZIP ${topHighConf.zip_code} leads high-confidence absentee ownership at ${pct(topHighConf.high_conf_investor_pct)}.`
  );
  setText("pressureAnswer",
    `ZIP ${topPressure.zip_code} has the highest modeled housing pressure score, with ` +
    `ZIP ${topRent.zip_code} showing the highest rent-to-income burden at ${ratioPct(topRent.rent_to_income_ratio)}.`
  );
  setText("sohAnswer",
    `Residents show the largest median SOH protection in ZIP ${topSoh.zip_code}, ` +
    `where the median differential is ${money(topSoh.median_soh_differential)}.`
  );
  setText("healthAnswer",
    `Among ACS-backed ZIPs, the investor-to-overdose correlation is ${corrFmt(corrInvOd)} ` +
    `and the rent-burden-to-overdose correlation is ${corrFmt(corrRentOd)}; ` +
    `ZIP ${topOd.zip_code} has the highest 2024 overdose rate at ${rate(topOd.overdose_rate_2024)}.`
  );
  setText("permitAnswer",
    `Development activity is concentrated in council district ${topPermit.council_district}, ` +
    `with ${number(topPermit.total_permits)} total permits and ${pct(topPermit.stalled_pct)} stalled.`
  );
}

/**
 * renderKpis builds the six executive scorecard cards.
 *
 * KPI calculation notes:
 *   - Parcel counts use SUM — straightforward aggregation.
 *   - Investor/owner percentages use weightedPct() — parcel-count-weighted
 *     average. This is the correct county-wide figure. A simple AVG would
 *     understate investor concentration because small inland ZIPs would
 *     receive equal weight to large coastal ZIPs.
 *   - Median SOH uses median() across ZIP-level medians — a median-of-medians,
 *     which is an approximation. The mart does not expose parcel-level SOH
 *     values, so a true county-wide median is not computable here.
 *   - Top pressure ZIP reflects the pressure_score composite computed in
 *     addPressureScores(), not a mart field.
 */
function renderKpis(rows) {
  if (!rows.length) {
    document.getElementById("kpiGrid").innerHTML =
      `<article class="kpi"><span>No matching ZIPs</span><strong>—</strong><small>Clear filters to restore the mart view</small></article>`;
    return;
  }

  const totalResidential = sum(rows, "residential_parcels");
  const topPressure      = topBy(rows, "pressure_score");

  const cards = [
    ["Residential parcels",   number(totalResidential),                      `${rows.length} ZIPs in current view`],
    ["All-investor exposure",  pct(weightedPct(rows, "all_investor_pct")),    "Parcel-weighted county rate"],
    ["Absentee confidence",    pct(weightedPct(rows, "high_conf_investor_pct")), "Foreign + out-of-state only"],
    ["Owner occupied",         pct(weightedPct(rows, "owner_occupied_pct")),  "Homestead-derived signal"],
    ["Median SOH gap",         money(median(rows.map((r) => r.median_soh_differential))), "Median of ZIP-level medians"],
    ["Top pressure ZIP",       topPressure.zip_code ?? "—",                  `${scoreFmt(topPressure.pressure_score)} composite score`],
  ];

  document.getElementById("kpiGrid").innerHTML = cards
    .map(([label, value, note]) => `
      <article class="kpi">
        <span>${label}</span>
        <strong>${value}</strong>
        <small>${note}</small>
      </article>`)
    .join("");
}

/**
 * renderRanking builds a horizontal bar chart for any mart metric.
 * Used by the leaderboard, investor, pressure, and SOH panels.
 *
 * @param {string} targetId  - DOM id of the container element
 * @param {Array}  rows      - filtered housing mart rows
 * @param {string} metric    - mart field name (must exist in metricMeta)
 * @param {number} limit     - max bars to show (default 12)
 */
function renderRanking(targetId, rows, metric, limit = 15) {
  const meta     = metricMeta[metric];
  const sortMode = document.getElementById("sortMode").value;
  const cleaned  = rows.filter((r) => Number.isFinite(r[metric]));
  cleaned.sort((a, b) => sortMode === "desc" ? b[metric] - a[metric] : a[metric] - b[metric]);
  const top      = cleaned.slice(0, limit);
  const maxValue = Math.max(...top.map((r) => Math.abs(r[metric])), 1);

  document.getElementById(targetId).innerHTML = top
    .map((r) => `
      <div class="bar-row" data-tip="ZIP ${r.zip_code}<br>${meta.label}: ${meta.fmt(r[metric])}<br>All investor: ${pct(r.all_investor_pct)}<br>Rent/income: ${ratioPct(r.rent_to_income_ratio)}<br>SOH: ${money(r.median_soh_differential)}">
        <strong>${r.zip_code}</strong>
        <div class="bar-track">
          <div class="bar-fill" style="width:${Math.max(3, Math.abs(r[metric]) / maxValue * 100)}%; background:${meta.color}"></div>
        </div>
        <span class="bar-value">${meta.fmt(r[metric])}</span>
      </div>`)
    .join("");

  bindTooltips();
}

function renderMix(rows) {
  const ownerMedian    = median(rows.map((r) => r.owner_occupied_pct));
  const allInvMedian   = median(rows.map((r) => r.all_investor_pct));
  const pressureMedian = median(rows.map((r) => r.pressure_score));
  const rentMedian     = median(rows.map((r) => r.rent_to_income_ratio));

  document.getElementById("mixChart").innerHTML = `
    <div class="donut" data-label="${scoreFmt(pressureMedian)} pressure"></div>
    <div class="legend-row"><strong><span class="dot"></span>Median all-investor</strong><span>${pct(allInvMedian)}</span></div>
    <div class="legend-row"><strong><span class="dot" style="background:#367b48"></span>Median owner occupied</strong><span>${pct(ownerMedian)}</span></div>
    <div class="legend-row"><strong><span class="dot" style="background:#b94a48"></span>Median rent burden</strong><span>${ratioPct(rentMedian)}</span></div>`;
}

function renderInvestorChart(rows) {
  renderRanking("investorChart", rows, "all_investor_pct");
}

/**
 * renderComposition breaks investor parcel counts by dbt classification type.
 * These are raw parcel counts from the mart, not percentages. The local
 * investor category includes seasonal residents and is lower confidence —
 * see mart documentation.
 */
function renderComposition(rows) {
  const items = [
    ["Out-of-state",  sum(rows, "out_of_state_count"),    "#2364aa"],
    ["Foreign",       sum(rows, "foreign_count"),          "#198f8a"],
    ["FL corporate",  sum(rows, "fl_corporate_count"),     "#c77a16"],
    ["Trust",         sum(rows, "trust_count"),            "#7a4f9f"],
    ["Local investor",sum(rows, "local_investor_count"),   "#b94a48"],
  ];

  document.getElementById("compositionChart").innerHTML = items
    .map(([label, val, color]) => `
      <div class="legend-row">
        <strong><span class="dot" style="background:${color}"></span>${label}</strong>
        <span>${number(val)}</span>
      </div>`)
    .join("");
}

function renderPressure(rows) {
  renderRanking("pressureChart", rows, "pressure_score");
}

/**
 * renderDriverPanel shows the metric breakdown for the single highest-pressure
 * ZIP in the current view. Helps stakeholders understand what is driving the
 * composite score for the top ZIP without reading the full table.
 */
function renderDriverPanel(rows) {
  if (!rows.length) {
    document.getElementById("driverPanel").innerHTML =
      `<div class="driver-hero"><span>No pressure profile</span><strong>—</strong><small>No ZIPs match the current filters</small></div>`;
    return;
  }

  const row = topBy(rows, "pressure_score");
  const drivers = [
    ["All investor ownership",  pct(row.all_investor_pct),           "#2364aa"],
    ["Rent-to-income ratio",    ratioPct(row.rent_to_income_ratio),  "#b94a48"],
    ["Renter-occupied ratio",   ratioPct(row.renter_occupied_ratio), "#198f8a"],
    ["Vacant-unit ratio",       ratioPct(row.vacant_unit_ratio),     "#7a4f9f"],
    ["Median SOH differential", money(row.median_soh_differential),  "#c77a16"],
    ["Owner occupied",          pct(row.owner_occupied_pct),         "#367b48"],
  ];

  document.getElementById("driverPanel").innerHTML = `
    <div class="driver-hero">
      <span>Highest pressure ZIP</span>
      <strong>${row.zip_code}</strong>
      <small>${scoreFmt(row.pressure_score)} composite score</small>
    </div>
    ${drivers.map(([label, val, color]) =>
      `<div class="legend-row"><strong><span class="dot" style="background:${color}"></span>${label}</strong><span>${val}</span></div>`
    ).join("")}`;
}

/**
 * renderZipTable builds the full intelligence table on the Pressure tab.
 * Sorted by the active metricSelect field. Columns are a fixed mart subset —
 * add columns here and in the columns array to expose additional mart fields.
 */
function renderZipTable(rows) {
  const metric   = getMetric();
  const sortMode = document.getElementById("sortMode").value;
  const sorted   = [...rows].sort((a, b) =>
    sortMode === "desc" ? fieldValue(b, metric) - fieldValue(a, metric) : fieldValue(a, metric) - fieldValue(b, metric)
  );

  const columns = [
    ["zip_code",                "ZIP"],
    ["pressure_score",          "Pressure Score"],
    ["residential_parcels",     "Residential Parcels"],
    ["all_investor_pct",        "All Investor"],
    ["high_conf_investor_pct",  "High Conf"],
    ["owner_occupied_pct",      "Owner Occupied"],
    ["median_soh_differential", "Median SOH"],
    ["rent_to_income_ratio",    "Rent/Income"],
    ["renter_occupied_ratio",   "Renter Share"],
    ["vacant_unit_ratio",       "Vacancy"],
    ["overdose_rate_2024",      "OD 2024"],
  ];

  document.getElementById("zipTable").innerHTML = `
    <thead>
      <tr>${columns.map(([, label]) => `<th>${label}</th>`).join("")}</tr>
    </thead>
    <tbody>
      ${sorted.map((r) =>
        `<tr>${columns.map(([key]) => `<td>${formatValue(key, r[key])}</td>`).join("")}</tr>`
      ).join("")}
    </tbody>`;
}

function renderSoh(rows) {
  renderRanking("sohChart", rows, "median_soh_differential");
}

/**
 * renderGaps shows ZIPs ranked by large_soh_gap_pct — the share of homestead
 * parcels whose SOH differential exceeds the large-gap threshold defined in
 * the dbt mart. A high percentage means a large share of long-term residents
 * face significant tax shock if displaced.
 */
function renderGaps(rows) {
  const top = rows
    .filter((r) => Number.isFinite(r.large_soh_gap_pct))
    .sort((a, b) => b.large_soh_gap_pct - a.large_soh_gap_pct)
    .slice(0, 10);

  document.getElementById("gapChart").innerHTML = top
    .map((r) => `<div class="mini-row"><strong>${r.zip_code}</strong><span>${pct(r.large_soh_gap_pct)} large SOH gap</span></div>`)
    .join("");
}

/**
 * renderScatter plots housing pressure score (x) against 2024 overdose rate (y).
 * Bubble radius scales with residential_parcels so larger ZIPs are visually
 * prominent. ZIPs missing either metric are excluded.
 *
 * The scatter is intentionally descriptive — it shows association, not causation.
 * The correlation coefficient shown in the health answer card provides the
 * quantified relationship.
 */
function renderScatter(rows) {
  const points  = rows.filter((r) => Number.isFinite(r.pressure_score) && Number.isFinite(r.overdose_rate_2024));
  const width   = 840;
  const height  = 430;
  const pad     = 54;
  const maxX    = Math.max(...points.map((r) => r.pressure_score), 1);
  const maxY    = Math.max(...points.map((r) => r.overdose_rate_2024), 1);
  const xPos    = (val) => pad + (val / maxX) * (width - pad * 1.6);
  const yPos    = (val) => height - pad - (val / maxY) * (height - pad * 1.6);

  document.getElementById("scatterChart").innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Scatter plot of housing pressure scores and 2024 overdose rates by ZIP code">
      ${[0.25, 0.5, 0.75, 1].map((t) =>
        `<line class="grid-line" x1="${pad}" x2="${width - pad / 2}" y1="${yPos(maxY * t)}" y2="${yPos(maxY * t)}"></line>`
      ).join("")}
      ${[0.25, 0.5, 0.75, 1].map((t) =>
        `<line class="grid-line" y1="${pad / 2}" y2="${height - pad}" x1="${xPos(maxX * t)}" x2="${xPos(maxX * t)}"></line>`
      ).join("")}
      <line class="axis" x1="${pad}" y1="${height - pad}" x2="${width - pad / 2}" y2="${height - pad}"></line>
      <line class="axis" x1="${pad}" y1="${pad / 2}"       x2="${pad}"            y2="${height - pad}"></line>
      <text x="${width / 2 - 95}" y="${height - 10}" fill="#667481" font-size="13">Housing pressure score</text>
      <text x="12" y="24" fill="#667481" font-size="13">2024 overdose rate</text>
      ${points.map((r) => `
        <circle class="point"
          cx="${xPos(r.pressure_score)}"
          cy="${yPos(r.overdose_rate_2024)}"
          r="${Math.max(5, Math.sqrt(r.residential_parcels) / 13)}"
          data-tip="ZIP ${r.zip_code}<br>Pressure: ${scoreFmt(r.pressure_score)}<br>Investor: ${pct(r.all_investor_pct)}<br>2024 OD: ${rate(r.overdose_rate_2024)}<br>Rent/income: ${ratioPct(r.rent_to_income_ratio)}">
        </circle>`
      ).join("")}
    </svg>`;

  bindTooltips();
}

/**
 * renderHealthList shows the 10 ZIPs with the largest overdose rate decline
 * (most negative overdose_rate_change). This highlights where the public
 * health trend is most positive within the 2021–2024 window.
 */
function renderHealthList(rows) {
  const top = rows
    .filter((r) => Number.isFinite(r.overdose_rate_change))
    .sort((a, b) => a.overdose_rate_change - b.overdose_rate_change)
    .slice(0, 10);

  document.getElementById("healthList").innerHTML = top
    .map((r) => `<div class="mini-row"><strong>${r.zip_code}</strong><span>${signedRate(r.overdose_rate_change)}</span></div>`)
    .join("");
}

/**
 * renderPermits renders both the permit volume bar chart and the queue health
 * legend from mart_permit_velocity. This panel is not filtered by ZIP search
 * because the permit mart is district-grain, not ZIP-grain.
 */
function renderPermits() {
  const rows = state.permits;
  const max  = Math.max(...rows.map((r) => r.total_permits), 1);

  document.getElementById("permitChart").innerHTML = rows
    .map((r) => `
      <div class="permit-row" data-tip="District ${r.council_district}<br>Total: ${number(r.total_permits)}<br>Residential: ${number(r.residential_permits)}<br>Commercial: ${number(r.commercial_permits)}<br>Stalled: ${pct(r.stalled_pct)}">
        <div>
          <strong>District ${r.council_district}</strong>
          <span>${number(r.residential_permits)} residential · ${number(r.commercial_permits)} commercial</span>
        </div>
        <div class="bar-track">
          <div class="bar-fill" style="width:${r.total_permits / max * 100}%; background:#198f8a"></div>
        </div>
        <span class="bar-value">${number(r.total_permits)}</span>
      </div>`)
    .join("");

  const statusItems = [
    ["Active permits", sum(rows, "active_permits"),        "#198f8a"],
    ["In review",      sum(rows, "in_review_permits"),     "#2364aa"],
    ["Intake",         sum(rows, "intake_permits"),        "#c77a16"],
    ["Held",           sum(rows, "held_permits"),          "#7a4f9f"],
    ["Stalled",        sum(rows, "stalled_permit_count"),  "#b94a48"],
  ];

  document.getElementById("permitStatus").innerHTML = statusItems
    .map(([label, val, color]) => `
      <div class="legend-row">
        <strong><span class="dot" style="background:${color}"></span>${label}</strong>
        <span>${number(val)}</span>
      </div>`)
    .join("");

  bindTooltips();
}

// ─── 6. Analytical helpers ────────────────────────────────────────────────────

/**
 * addPressureScores computes a normalized composite pressure score for each
 * housing mart row and returns a new array with a pressure_score field added.
 *
 * ZIPs missing all three ACS fields (rent_to_income_ratio, renter_occupied_ratio,
 * vacant_unit_ratio) receive pressure_score = null and are excluded from
 * pressure-ranked views. Missing individual fields are treated as 0 after
 * normalization, which understates their contribution — acceptable for the
 * current dataset where only 5 of 29 ZIPs lack ACS data.
 *
 * Weights reflect domain priorities (see handoff documentation):
 *   all_investor_pct is the primary signal (0.32).
 *   rent_to_income_ratio captures affordability stress (0.22).
 *   renter_occupied_ratio proxies ownership stability (0.14).
 *   median_soh_differential captures displacement risk (0.12).
 *   vacant_unit_ratio captures market slack (0.10).
 *   owner_occupied_pct (inverted) reinforces the investor signal (0.10).
 */
function addPressureScores(rows) {
  const pressureMetrics = [
    "all_investor_pct",
    "rent_to_income_ratio",
    "renter_occupied_ratio",
    "vacant_unit_ratio",
    "median_soh_differential",
  ];

  // Compute min/max for each metric across all rows (before filtering)
  // so scores are stable and do not change when ZIP filters are applied.
  const ranges = Object.fromEntries(
    pressureMetrics.map((key) => {
      const nums = rows.map((r) => r[key]).filter(Number.isFinite);
      return [key, { min: Math.min(...nums), max: Math.max(...nums) }];
    })
  );

  const ownerRange = (() => {
    const nums = rows.map((r) => r.owner_occupied_pct).filter(Number.isFinite);
    return { min: Math.min(...nums), max: Math.max(...nums) };
  })();

  const ACS_FIELDS = ["rent_to_income_ratio", "renter_occupied_ratio", "vacant_unit_ratio"];

  return rows.map((row) => {
    // Exclude ZIPs where all ACS fields are missing to avoid misleadingly
    // low scores. These ZIPs still appear in non-pressure views.
    const missingAllAcs = ACS_FIELDS.every((f) => !Number.isFinite(row[f]));
    if (missingAllAcs) return { ...row, pressure_score: null };

    const score = (
      norm(row.all_investor_pct,          ranges.all_investor_pct)          * 0.32 +
      norm(row.rent_to_income_ratio,       ranges.rent_to_income_ratio)       * 0.22 +
      norm(row.renter_occupied_ratio,      ranges.renter_occupied_ratio)      * 0.14 +
      norm(row.vacant_unit_ratio,          ranges.vacant_unit_ratio)          * 0.10 +
      norm(row.median_soh_differential,    ranges.median_soh_differential)    * 0.12 +
      (1 - norm(row.owner_occupied_pct,    ownerRange))                       * 0.10
    ) * 100;

    return { ...row, pressure_score: Number(score.toFixed(1)) };
  });
}

/**
 * norm min-max normalizes a single value within a range.
 * Returns 0 for non-finite values (null, NaN, Infinity) so missing fields
 * contribute zero to the pressure score rather than NaN-propagating.
 */
function norm(val, range) {
  if (!Number.isFinite(val) || range.max === range.min) return 0;
  return (val - range.min) / (range.max - range.min);
}

/**
 * weightedPct computes a parcel-count-weighted average of a percentage field
 * across the visible row set. This produces the true county-wide rate.
 *
 * Example: ZIP A has 1,000 parcels at 40% investor-owned;
 *          ZIP B has 10,000 parcels at 10% investor-owned.
 *          Simple AVG = 25%. Weighted = (400 + 1000) / 11000 = 12.7%.
 *          The weighted figure is correct; the simple AVG overstates by 2×.
 *
 * @param {Array}  rows   - filtered housing mart rows
 * @param {string} metric - mart field storing a percentage (e.g. "all_investor_pct")
 * @returns {number|null} weighted percentage value, or null if no valid rows
 */
function weightedPct(rows, metric) {
  const valid = rows.filter((r) => Number.isFinite(r[metric]) && Number.isFinite(r.residential_parcels));
  const denom = valid.reduce((acc, r) => acc + r.residential_parcels, 0);
  if (!denom) return null;
  return valid.reduce((acc, r) => acc + r[metric] * r.residential_parcels, 0) / denom;
}

/**
 * correlation computes the Pearson correlation coefficient between two
 * mart fields across the visible row set. Rows missing either field are
 * excluded. Returns null if fewer than 3 valid pairs exist.
 */
function correlation(rows, xKey, yKey) {
  const pairs = rows
    .filter((r) => Number.isFinite(r[xKey]) && Number.isFinite(r[yKey]))
    .map((r) => [r[xKey], r[yKey]]);

  if (pairs.length < 3) return null;

  const xMean   = pairs.reduce((s, [x]) => s + x, 0) / pairs.length;
  const yMean   = pairs.reduce((s, [, y]) => s + y, 0) / pairs.length;
  const num     = pairs.reduce((s, [x, y]) => s + (x - xMean) * (y - yMean), 0);
  const xDenom  = Math.sqrt(pairs.reduce((s, [x]) => s + (x - xMean) ** 2, 0));
  const yDenom  = Math.sqrt(pairs.reduce((s, [, y]) => s + (y - yMean) ** 2, 0));

  return xDenom && yDenom ? num / (xDenom * yDenom) : null;
}

function topBy(rows, key) {
  return [...rows].filter((r) => Number.isFinite(r[key])).sort((a, b) => b[key] - a[key])[0] ?? {};
}

function sum(rows, key) {
  return rows.reduce((acc, r) => acc + (Number(r[key]) || 0), 0);
}

function median(values) {
  const nums = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (!nums.length) return null;
  const mid = Math.floor(nums.length / 2);
  return nums.length % 2 ? nums[mid] : (nums[mid - 1] + nums[mid]) / 2;
}

function fieldValue(row, key) {
  return Number.isFinite(row[key]) ? row[key] : -Infinity;
}

// ─── 7. CSV utilities ─────────────────────────────────────────────────────────

/**
 * loadCsv fetches a CSV file and returns a parsed array of plain objects.
 * Throws if the HTTP response is not ok (404, 500, etc.) so the Promise.all
 * boot sequence surfaces the error rather than silently rendering empty state.
 */
async function loadCsv(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Could not load ${path} (HTTP ${res.status})`);
  return parseCsv(await res.text());
}

/**
 * parseCsv is a minimal RFC-4180-aware CSV parser. It handles:
 *   - Quoted fields containing embedded commas
 *   - Quoted fields containing escaped double-quotes ("")
 *   - Windows-style line endings (\r\n)
 *
 * Supabase CSV exports do not currently produce commas inside numeric fields,
 * but this parser is safe against future schema changes that add string columns
 * (e.g. city name, classification label). The naive split(",") approach used
 * previously would silently corrupt those rows.
 *
 * @param  {string} text - raw CSV string
 * @returns {Array}       array of plain objects keyed by header row
 */
function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  const headers = splitCsvRow(lines.shift());
  return lines.map((line) => {
    const cells = splitCsvRow(line);
    return Object.fromEntries(headers.map((h, i) => [h, cells[i] ?? ""]));
  });
}

/**
 * splitCsvRow parses a single CSV line respecting RFC-4180 quoting rules.
 * Quoted fields may contain commas and escaped double-quotes ("").
 */
function splitCsvRow(line) {
  const fields = [];
  let cur = "";
  let inQuote = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuote) {
      if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; }
      else if (ch === '"') { inQuote = false; }
      else { cur += ch; }
    } else {
      if (ch === '"') { inQuote = true; }
      else if (ch === ',') { fields.push(cur); cur = ""; }
      else { cur += ch; }
    }
  }
  fields.push(cur);
  return fields;
}

/**
 * typeRow converts a parsed CSV row (all string values) into typed JS values.
 * Conversion rules:
 *   "true" / "false"  → boolean
 *   "null" / ""       → null
 *   numeric string    → number  (except zip_code, which stays a string)
 *   anything else     → string
 *
 * zip_code is kept as a string to preserve leading zeros and prevent
 * accidental arithmetic on what is a label, not a quantity.
 */
function typeRow(row) {
  return Object.fromEntries(
    Object.entries(row).map(([key, val]) => {
      if (val === "true")             return [key, true];
      if (val === "false")            return [key, false];
      if (val === "null" || val === "") return [key, null];
      const num = Number(val);
      return Number.isFinite(num) && key !== "zip_code" ? [key, num] : [key, val];
    })
  );
}

// ─── 8. Format utilities ──────────────────────────────────────────────────────

/** Formats a whole-number percentage field (e.g. 17.6 → "17.6%"). */
function pct(val) {
  return Number.isFinite(val) ? `${val.toFixed(1)}%` : "n/a";
}

/**
 * Formats a 0–1 decimal ratio as a percentage (e.g. 0.353 → "35.3%").
 * Used for rent_to_income_ratio, renter_occupied_ratio, vacant_unit_ratio,
 * which are stored as decimals in the mart.
 */
function ratioPct(val) {
  return Number.isFinite(val) ? `${(val * 100).toFixed(1)}%` : "n/a";
}

/** Formats a dollar value with no decimal places (e.g. 110858 → "$110,858"). */
function money(val) {
  return Number.isFinite(val)
    ? new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(val)
    : "n/a";
}

/** Formats an overdose rate to two decimal places with unit label. */
function rate(val) {
  return Number.isFinite(val) ? `${val.toFixed(2)} / 1k` : "n/a";
}

/** Formats an overdose rate change with explicit +/- sign. */
function signedRate(val) {
  return Number.isFinite(val) ? `${val > 0 ? "+" : ""}${val.toFixed(2)} / 1k` : "n/a";
}

/** Formats a pressure score to one decimal place. */
function scoreFmt(val) {
  return Number.isFinite(val) ? `${val.toFixed(1)}` : "n/a";
}

/** Formats a large integer with locale-appropriate thousand separators. */
function number(val) {
  return Number.isFinite(val)
    ? new Intl.NumberFormat("en-US").format(Math.round(val))
    : "n/a";
}

/**
 * corrFmt formats a Pearson correlation coefficient into a human-readable
 * strength + direction label with the raw value in parentheses.
 * Thresholds: |r| ≥ 0.6 = strong, ≥ 0.35 = moderate, < 0.35 = weak.
 */
function corrFmt(val) {
  if (!Number.isFinite(val)) return "n/a";
  const strength  = Math.abs(val) >= 0.6 ? "strong" : Math.abs(val) >= 0.35 ? "moderate" : "weak";
  const direction = val >= 0 ? "positive" : "negative";
  return `${strength} ${direction} (${val.toFixed(2)})`;
}

/**
 * formatValue dispatches to the correct format function based on field name.
 * Used by the ZIP intelligence table to format heterogeneous mart columns.
 */
function formatValue(key, val) {
  if (key === "pressure_score")                                          return scoreFmt(val);
  if (key.includes("_pct"))                                             return pct(val);
  if (["rent_to_income_ratio", "renter_occupied_ratio", "vacant_unit_ratio"].includes(key)) return ratioPct(val);
  if (key.includes("soh"))                                              return money(val);
  if (key.includes("overdose"))                                         return rate(val);
  if (typeof val === "number")                                          return number(val);
  return val ?? "n/a";
}

// ─── 9. UI helpers ────────────────────────────────────────────────────────────

function setText(id, text) {
  document.getElementById(id).textContent = text;
}

/**
 * bindTooltips attaches mousemove/mouseleave handlers to any element with a
 * data-tip attribute. Called after each render that produces tooltip-bearing
 * elements. The tooltip element itself is a fixed-position div in the HTML.
 */
function bindTooltips() {
  const tooltip = document.getElementById("tooltip");
  document.querySelectorAll("[data-tip]").forEach((node) => {
    node.addEventListener("mousemove", (e) => {
      tooltip.hidden  = false;
      tooltip.innerHTML = node.dataset.tip;
      tooltip.style.left = `${e.clientX + 14}px`;
      tooltip.style.top  = `${e.clientY + 14}px`;
    });
    node.addEventListener("mouseleave", () => { tooltip.hidden = true; });
  });
}

// ─── 9. Export ────────────────────────────────────────────────────────────────

/**
 * exportSummary generates a CSV snapshot of the current filtered view and
 * triggers a browser download. All percentage fields use weightedPct() for
 * consistency with the KPI cards — earlier versions used median(), which
 * produced different values for the same metric in different outputs.
 */
function exportSummary() {
  const rows       = getVisibleRows();
  const topPressure = topBy(rows, "pressure_score");

  const summary = [
    ["visible_zips",               rows.length],
    ["residential_parcels",        sum(rows, "residential_parcels")],
    ["top_pressure_zip",           topPressure.zip_code ?? "n/a"],
    ["top_pressure_score",         scoreFmt(topPressure.pressure_score)],
    ["weighted_all_investor_pct",  pct(weightedPct(rows, "all_investor_pct"))],
    ["weighted_high_conf_pct",     pct(weightedPct(rows, "high_conf_investor_pct"))],
    ["weighted_owner_occupied_pct",pct(weightedPct(rows, "owner_occupied_pct"))],
    ["median_soh_differential",    money(median(rows.map((r) => r.median_soh_differential)))],
    ["investor_overdose_correlation", corrFmt(correlation(rows, "all_investor_pct", "overdose_rate_2024"))],
  ];

  const csv  = "metric,value\n" + summary.map(([k, v]) => `${k},${csvCell(v)}`).join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url  = URL.createObjectURL(blob);
  const a    = Object.assign(document.createElement("a"), { href: url, download: "volusia_pressure_summary.csv" });
  a.click();
  URL.revokeObjectURL(url);
}

function csvCell(val) {
  const text = String(val ?? "");
  return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}