/**
 * CrimeLens Spark - Client Application Logic
 * Integrates Leaflet Chicago Map, Chart.js Visuals, REST API calls,
 * and PySpark Random Forest interactive prediction.
 */

let mapInstance = null;
let crimeChartInstance = null;
let featImpChartInstance = null;
let currentPageOffset = 0;
const PAGE_SIZE = 25;

document.addEventListener("DOMContentLoaded", () => {
  initNavigation();
  initOverviewData();
  initMLPredictor();
  initDatabaseExplorer();
  initDiagnosticsData();
  initTerminalRunner();
  initSqlStudio();
  initServerTelemetry();
});

/* ==========================================================================
   1. Tab Navigation
   ========================================================================== */
function initNavigation() {
  const navItems = document.querySelectorAll(".nav-item");
  const tabPanes = document.querySelectorAll(".tab-pane");
  const titleEl = document.getElementById("currentTabTitle");

  const tabTitles = {
    "overview": "Overview & Geospatial Hotspots",
    "terminal-proofs": "Terminal Execution Proofs & Logs (15 Marks Evidence)",
    "rdd-engine": "Scala + Spark RDD Engine Mechanics",
    "ml-predictor": "PySpark ML Random Forest Predictor",
    "database": "City of Chicago Data Lake Explorer & SQL Studio",
    "diagnostics": "PySpark Model Diagnostics & Metrics",
    "rubric": "Review 3 Rubrics & Team Contributions"
  };

  navItems.forEach(item => {
    item.addEventListener("click", () => {
      const targetTab = item.getAttribute("data-tab");

      navItems.forEach(n => n.classList.remove("active"));
      tabPanes.forEach(p => p.classList.remove("active"));

      item.classList.add("active");
      const activePane = document.getElementById(`tab-${targetTab}`);
      if (activePane) activePane.classList.add("active");

      if (titleEl && tabTitles[targetTab]) {
        titleEl.textContent = tabTitles[targetTab];
      }

      // If overview tab is activated, invalidate Leaflet map size
      if (targetTab === "overview" && mapInstance) {
        setTimeout(() => mapInstance.invalidateSize(), 200);
      }
    });
  });
}

/* ==========================================================================
   2. Overview & Map Integration
   ========================================================================== */
async function initOverviewData() {
  try {
    const [kpiRes, hotspotsRes, typesRes] = await Promise.all([
      fetch("/api/kpis").then(r => r.json()),
      fetch("/api/hotspots?limit=10").then(r => r.json()),
      fetch("/api/crime-types?limit=8").then(r => r.json())
    ]);

    // Populate KPIs
    document.getElementById("kpiTotal").textContent = Number(kpiRes.total_records).toLocaleString();
    document.getElementById("kpiArrests").textContent = Number(kpiRes.arrests).toLocaleString();

    // Render Leaflet Map
    renderChicagoMap(hotspotsRes.districts);

    // Render Crime Types Bar Chart
    renderCrimeTypeChart(typesRes.crime_types);

    // Render Priority Districts Table
    renderHotspotsTable(hotspotsRes.districts);
  } catch (err) {
    console.error("Error loading overview telemetry:", err);
  }
}

function renderChicagoMap(districts) {
  const mapContainer = document.getElementById("chicagoMap");
  if (!mapContainer) return;

  if (mapInstance) {
    mapInstance.remove();
  }

  // Chicago Downtown Coordinates
  mapInstance = L.map("chicagoMap", {
    zoomControl: true,
    scrollWheelZoom: false
  }).setView([41.8500, -87.6500], 11);

  // High-contrast Dark Tiles (CartoDB Dark Matter)
  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    attribution: '&copy; <a href="https://carto.com/">CartoDB</a> | Chicago Police Dept',
    maxZoom: 18
  }).addTo(mapInstance);

  districts.forEach((d, idx) => {
    if (!d.lat || !d.lon) return;

    // Radius proportional to crime count
    const radius = Math.min(26, Math.max(12, Math.sqrt(d.count) * 0.9));
    const isTop3 = idx < 3;
    const color = isTop3 ? "#00E5FF" : "#3B82F6";

    const circle = L.circleMarker([d.lat, d.lon], {
      radius: radius,
      fillColor: color,
      color: "#FFFFFF",
      weight: 1.5,
      opacity: 0.9,
      fillOpacity: 0.75
    }).addTo(mapInstance);

    circle.bindPopup(`
      <div style="font-family: 'Inter', sans-serif; padding: 6px; color: #0F172A;">
        <h4 style="margin: 0 0 4px 0; font-size: 14px; font-weight: 700; color: #0284C7;">
          Police District ${d.district}
        </h4>
        <p style="margin: 0; font-size: 12px; line-height: 1.4;">
          <strong>Total Incidents:</strong> ${d.count.toLocaleString()}<br>
          <strong>Apprehensions:</strong> ${d.arrests.toLocaleString()}<br>
          <strong>Clearance:</strong> ${((d.arrests / d.count) * 100).toFixed(1)}%<br>
          <strong>Rank:</strong> #${idx + 1} Metropolitan Hotspot
        </p>
      </div>
    `);
  });
}

function renderCrimeTypeChart(types) {
  const ctx = document.getElementById("crimeTypeChart");
  if (!ctx) return;

  if (crimeChartInstance) crimeChartInstance.destroy();

  const labels = types.map(t => t.primary_type);
  const data = types.map(t => t.count);

  crimeChartInstance = new Chart(ctx, {
    type: "bar",
    data: {
      labels: labels,
      datasets: [{
        label: "Incident Volume",
        data: data,
        backgroundColor: "rgba(0, 229, 255, 0.65)",
        borderColor: "#00E5FF",
        borderWidth: 1.5,
        borderRadius: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "#0B0F19",
          titleFont: { family: "Outfit", size: 13 },
          bodyFont: { family: "Inter", size: 12 },
          borderColor: "rgba(0, 229, 255, 0.4)",
          borderWidth: 1
        }
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: { color: "#94A3B8", font: { size: 10 } }
        },
        y: {
          grid: { color: "rgba(255, 255, 255, 0.05)" },
          ticks: { color: "#94A3B8", font: { size: 10 } }
        }
      }
    }
  });
}

function renderHotspotsTable(districts) {
  const tbody = document.querySelector("#hotspotsTable tbody");
  if (!tbody) return;

  tbody.innerHTML = districts.map((d, idx) => {
    const clearance = ((d.arrests / d.count) * 100).toFixed(1);
    const priorityBadge = idx < 2 
      ? '<span class="badge danger">CRITICAL PRIORITY</span>'
      : idx < 5 
        ? '<span class="badge warning">HIGH PATROL</span>'
        : '<span class="badge cyan">STANDARD PATROL</span>';

    return `
      <tr>
        <td><strong>#${idx + 1}</strong></td>
        <td>District ${d.district}</td>
        <td><strong>${d.count.toLocaleString()}</strong></td>
        <td>${d.arrests.toLocaleString()}</td>
        <td>${clearance}%</td>
        <td>${priorityBadge}</td>
      </tr>
    `;
  }).join("");
}

/* ==========================================================================
   3. PySpark Machine Learning Predictor
   ========================================================================== */
function initMLPredictor() {
  const form = document.getElementById("predictForm");
  if (!form) return;

  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const payload = {
      primary_type: document.getElementById("primary_type").value,
      location_description: document.getElementById("location_description").value,
      district: document.getElementById("district").value,
      beat: document.getElementById("beat").value,
      domestic: document.getElementById("domestic").checked
    };

    const btn = document.getElementById("btnPredict");
    btn.disabled = true;
    btn.innerHTML = "<span>Analyzing Random Forest Trees...</span>";

    try {
      const res = await fetch("/api/predict", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      displayPredictionResult(data);
    } catch (err) {
      console.error("Prediction API error:", err);
    } finally {
      btn.disabled = false;
      btn.innerHTML = "<span>⚡ Execute PySpark Random Forest Prediction</span>";
    }
  });
}

function displayPredictionResult(data) {
  const resBox = document.getElementById("predictionResult");
  const resBadge = document.getElementById("resBadge");
  const resLabel = document.getElementById("resLabel");
  const resDesc = document.getElementById("resDesc");
  const resPct = document.getElementById("resPct");
  const resConfidence = document.getElementById("resConfidence");
  const resLatency = document.getElementById("resLatency");
  const gaugeArc = document.getElementById("gaugeArc");
  const weightsList = document.getElementById("featureWeightsList");

  resBox.style.display = "block";

  const isArrest = data.prediction === 1;
  const pct = data.probability_pct;

  resLabel.textContent = data.label;
  resPct.textContent = `${pct}%`;
  resConfidence.textContent = data.confidence;
  resLatency.textContent = `${data.latency_ms} ms`;

  if (isArrest) {
    resBadge.className = "badge success";
    resBadge.textContent = "ARREST PREDICTED";
    resDesc.textContent = "High probability of immediate suspect apprehension based on offense characteristics and on-scene physical custody likelihood.";
    gaugeArc.setAttribute("stroke", "#10B981");
  } else {
    resBadge.className = "badge warning";
    resBadge.textContent = "UNAPPREHENDED (OPEN CASE)";
    resDesc.textContent = "Low likelihood of immediate arrest. Incident requires detective bureau follow-up, forensic evidence, and surveillance review.";
    gaugeArc.setAttribute("stroke", "#F59E0B");
  }

  // Circular gauge circumference = 2 * PI * 56 = ~351.85
  const circumference = 351.85;
  const offset = circumference - (pct / 100) * circumference;
  gaugeArc.style.transition = "stroke-dashoffset 0.8s ease-in-out";
  gaugeArc.setAttribute("stroke-dashoffset", offset);

  // Render Feature Contributions
  weightsList.innerHTML = data.feature_contributions.map(fc => `
    <div style="background: rgba(15, 23, 42, 0.8); padding: 8px 12px; border-radius: 6px; border: 1px solid rgba(255,255,255,0.06);">
      <div style="display: flex; justify-content: space-between; margin-bottom: 2px;">
        <span style="color: #F1F5F9; font-weight: 500;">${fc.feature}</span>
        <strong style="color: var(--accent-cyan);">${fc.weight_pct}%</strong>
      </div>
      <span style="font-size: 10.5px; color: ${fc.direction === 'Positive' ? '#10B981' : '#94A3B8'};">
        Direction: ${fc.direction} Impact
      </span>
    </div>
  `).join("");

  resBox.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

/* ==========================================================================
   4. Database Explorer (SQLite)
   ========================================================================== */
function initDatabaseExplorer() {
  const btnFilter = document.getElementById("btnFilterData");
  const btnPrev = document.getElementById("btnPrevPage");
  const btnNext = document.getElementById("btnNextPage");

  if (btnFilter) {
    btnFilter.addEventListener("click", () => {
      currentPageOffset = 0;
      loadCrimesTable();
    });
  }

  if (btnPrev) {
    btnPrev.addEventListener("click", () => {
      if (currentPageOffset >= PAGE_SIZE) {
        currentPageOffset -= PAGE_SIZE;
        loadCrimesTable();
      }
    });
  }

  if (btnNext) {
    btnNext.addEventListener("click", () => {
      currentPageOffset += PAGE_SIZE;
      loadCrimesTable();
    });
  }

  // Initial load
  loadCrimesTable();
}

async function loadCrimesTable() {
  const q = document.getElementById("searchQuery")?.value || "";
  const district = document.getElementById("filterDistrict")?.value || "";
  const arrest = document.getElementById("filterArrest")?.value || "";
  const tbody = document.querySelector("#crimesTable tbody");
  const pageInfo = document.getElementById("pageInfo");

  if (!tbody) return;
  tbody.innerHTML = `<tr><td colspan="8" style="text-align:center; padding: 20px;">Querying SQLite Data Lake...</td></tr>`;

  try {
    const url = `/api/crimes?q=${encodeURIComponent(q)}&district=${encodeURIComponent(district)}&arrest=${encodeURIComponent(arrest)}&limit=${PAGE_SIZE}&offset=${currentPageOffset}`;
    const res = await fetch(url);
    const data = await res.json();

    if (!data.records || data.records.length === 0) {
      tbody.innerHTML = `<tr><td colspan="8" style="text-align:center; padding: 20px; color: var(--text-dim);">No incidents matching query criteria.</td></tr>`;
      return;
    }

    tbody.innerHTML = data.records.map(r => `
      <tr>
        <td><code>${r.id}</code></td>
        <td>${r.case_number}</td>
        <td>${r.date ? r.date.split("T")[0] : "2026-08"}</td>
        <td><strong>${r.primary_type}</strong></td>
        <td style="max-width: 180px; text-overflow: ellipsis; overflow: hidden; white-space: nowrap;">${r.location_description || "STREET"}</td>
        <td>District ${r.district}</td>
        <td>Beat ${r.beat}</td>
        <td>
          ${r.arrest === 1 
            ? '<span class="badge success">Arrested (1)</span>' 
            : '<span class="badge danger">Open (0)</span>'}
        </td>
      </tr>
    `).join("");

    if (pageInfo) {
      const start = currentPageOffset + 1;
      const end = Math.min(currentPageOffset + data.records.length, data.total_records);
      pageInfo.textContent = `Showing ${start} to ${end} of ${data.total_records.toLocaleString()} records`;
    }
  } catch (err) {
    console.error("Error loading crimes table:", err);
  }
}

/* ==========================================================================
   5. Diagnostics & Feature Importance Chart
   ========================================================================== */
async function initDiagnosticsData() {
  try {
    const res = await fetch("/api/diagnostics");
    const data = await res.json();

    const ctx = document.getElementById("featureImpChart");
    if (!ctx) return;

    if (featImpChartInstance) featImpChartInstance.destroy();

    const features = data.feature_importance.features;
    const labels = features.map(f => f.name);
    const weights = features.map(f => f.importance * 100);

    featImpChartInstance = new Chart(ctx, {
      type: "bar",
      data: {
        labels: labels,
        datasets: [{
          label: "Importance Weight (%)",
          data: weights,
          backgroundColor: "rgba(99, 102, 241, 0.75)",
          borderColor: "#6366F1",
          borderWidth: 1.5,
          borderRadius: 6
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "#0B0F19",
            borderColor: "#6366F1",
            borderWidth: 1
          }
        },
        scales: {
          x: {
            grid: { color: "rgba(255, 255, 255, 0.05)" },
            ticks: { color: "#94A3B8", callback: v => `${v}%` }
          },
          y: {
            grid: { display: false },
            ticks: { color: "#F1F5F9", font: { size: 11 } }
          }
        }
      }
    });
  } catch (err) {
    console.error("Error loading diagnostics:", err);
  }
}

/* ==========================================================================
   6. Live Terminal Simulator & Execution Runner
   ========================================================================== */
let activeTerminalCmd = "scala_rdd";

function initTerminalRunner() {
  const cmdBtns = document.querySelectorAll(".cmd-btn[data-cmd]");
  const runBtn = document.getElementById("btnRunTerminal");
  const copyBtn = document.getElementById("btnCopyTerm");
  const clearBtn = document.getElementById("btnClearTerm");

  cmdBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      cmdBtns.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      activeTerminalCmd = btn.getAttribute("data-cmd");
      runTerminalCommand(activeTerminalCmd);
    });
  });

  if (runBtn) {
    runBtn.addEventListener("click", () => {
      runTerminalCommand(activeTerminalCmd);
    });
  }

  if (copyBtn) {
    copyBtn.addEventListener("click", () => {
      const outputEl = document.getElementById("termOutput");
      if (outputEl) {
        navigator.clipboard.writeText(outputEl.innerText).then(() => {
          const original = copyBtn.innerHTML;
          copyBtn.innerHTML = "<span>✓ Copied!</span>";
          setTimeout(() => copyBtn.innerHTML = original, 2000);
        });
      }
    });
  }

  if (clearBtn) {
    clearBtn.addEventListener("click", () => {
      const outputEl = document.getElementById("termOutput");
      if (outputEl) outputEl.innerHTML = '<div class="term-line term-dim">Console cleared. Select a pipeline above and click "Run Stream".</div>';
    });
  }

  // Pre-load default Scala RDD execution stream
  runTerminalCommand("scala_rdd");
}

async function runTerminalCommand(commandName) {
  const outputEl = document.getElementById("termOutput");
  if (!outputEl) return;

  outputEl.innerHTML = `
    <div class="term-line term-dim">[*] Connecting to Spark execution runtime...</div>
    <div class="term-line term-cyan">[*] Requesting real-time execution stream for '${commandName}'...</div>
  `;

  try {
    const res = await fetch("/api/terminal/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command: commandName })
    });
    const data = await res.json();

    if (!res.ok || data.error) {
      outputEl.innerHTML += `<div class="term-line term-red">[ERROR] ${data.error || "Failed to execute"}</div>`;
      return;
    }

    outputEl.innerHTML = "";
    const lines = data.lines || [];

    // Stream lines into terminal with staggered rendering
    lines.forEach((line, index) => {
      setTimeout(() => {
        const lineDiv = document.createElement("div");
        lineDiv.className = "term-line";

        // Syntax styling
        if (line.startsWith("hadoop@") || line.startsWith("aksharsakhi@") || line.startsWith("scala>")) {
          lineDiv.classList.add("term-cyan", "term-bold");
        } else if (line.includes("[SUCCESS]") || line.includes("100% Satisfied") || line.includes("[✓]")) {
          lineDiv.classList.add("term-green", "term-bold");
        } else if (line.includes("[STEP") || line.includes("[PASS") || line.includes("====================")) {
          lineDiv.classList.add("term-blue", "term-bold");
        } else if (line.includes("WARN") || line.includes("repartition")) {
          lineDiv.classList.add("term-yellow");
        } else if (line.includes("ERROR") || line.includes("Failed")) {
          lineDiv.classList.add("term-red");
        } else if (line.includes("->") || line.includes(">>")) {
          lineDiv.classList.add("term-cyan");
        }

        lineDiv.textContent = line;
        outputEl.appendChild(lineDiv);
        outputEl.scrollTop = outputEl.scrollHeight;
      }, index * 25);
    });

  } catch (err) {
    outputEl.innerHTML += `<div class="term-line term-red">[FATAL] Network error: ${err.message}</div>`;
  }
}

/* ==========================================================================
   7. Interactive SQL Studio
   ========================================================================== */
function initSqlStudio() {
  const chips = document.querySelectorAll(".sql-chip");
  const sqlInput = document.getElementById("sqlInput");
  const btnRunSql = document.getElementById("btnRunSql");

  chips.forEach(chip => {
    chip.addEventListener("click", () => {
      const sql = chip.getAttribute("data-sql");
      if (sqlInput && sql) {
        sqlInput.value = sql;
        executeCustomSql(sql);
      }
    });
  });

  if (btnRunSql && sqlInput) {
    btnRunSql.addEventListener("click", () => {
      executeCustomSql(sqlInput.value.trim());
    });
  }

  // Pre-load default query
  if (sqlInput && sqlInput.value) {
    executeCustomSql(sqlInput.value.trim());
  }
}

async function executeCustomSql(query) {
  const resultBox = document.getElementById("sqlResultBox");
  const thead = document.getElementById("sqlResultThead");
  const tbody = document.getElementById("sqlResultTbody");
  const infoEl = document.getElementById("sqlResultInfo");
  const latencyBadge = document.getElementById("sqlLiveLatency");

  if (!query) return;

  const t0 = performance.now();

  try {
    const res = await fetch("/api/sql/execute", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: query })
    });
    const elapsed = (performance.now() - t0).toFixed(1);
    const data = await res.json();

    if (latencyBadge) {
      latencyBadge.textContent = `Latency: ${elapsed} ms • SQLite B-Tree Live`;
    }

    if (!res.ok || data.error) {
      if (resultBox) resultBox.style.display = "block";
      if (infoEl) infoEl.innerHTML = `<span style="color: var(--accent-red);">SQL Error: ${data.error || "Query failed"}</span>`;
      if (thead) thead.innerHTML = "";
      if (tbody) tbody.innerHTML = "";
      return;
    }

    if (resultBox) resultBox.style.display = "block";
    if (infoEl) {
      infoEl.innerHTML = `Query Succeeded: <strong style="color: var(--accent-green);">${data.row_count} rows returned</strong> in <strong style="color: var(--accent-cyan);">${elapsed} ms</strong>`;
    }

    // Build Table Headers
    if (thead) {
      thead.innerHTML = `<tr>${data.columns.map(c => `<th>${c}</th>`).join("")}</tr>`;
    }

    // Build Table Rows
    if (tbody) {
      tbody.innerHTML = data.rows.map(row => {
        return `<tr>${data.columns.map(c => `<td>${row[c] !== null ? row[c] : "NULL"}</td>`).join("")}</tr>`;
      }).join("");
    }

  } catch (err) {
    if (resultBox) resultBox.style.display = "block";
    if (infoEl) infoEl.innerHTML = `<span style="color: var(--accent-red);">Network Error: ${err.message}</span>`;
  }
}

/* ==========================================================================
   8. Server Telemetry & Lightbox Handlers
   ========================================================================== */
async function initServerTelemetry() {
  const pingEl = document.getElementById("apiPingText");
  const t0 = performance.now();
  try {
    const res = await fetch("/api/server-info");
    const elapsed = (performance.now() - t0).toFixed(1);
    if (pingEl && res.ok) {
      pingEl.innerHTML = `⚡ REST API: <strong style="color: var(--accent-green);">${elapsed} ms</strong>`;
    }
  } catch (e) {
    if (pingEl) pingEl.innerHTML = `⚡ REST API: <strong style="color: var(--accent-red);">Offline</strong>`;
  }
}

window.openLightbox = function(imgSrc, title) {
  const modal = document.getElementById("proofLightbox");
  const modalImg = document.getElementById("modalImg");
  const modalTitle = document.getElementById("modalTitle");

  if (modal && modalImg) {
    modalImg.src = imgSrc;
    if (modalTitle && title) modalTitle.textContent = title;
    modal.classList.add("active");
  }
};

window.closeLightbox = function(e) {
  const modal = document.getElementById("proofLightbox");
  if (modal) modal.classList.remove("active");
};

// Keyboard escape to close lightbox
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    const modal = document.getElementById("proofLightbox");
    if (modal && modal.classList.contains("active")) {
      modal.classList.remove("active");
    }
  }
});
