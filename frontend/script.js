// frontend/script.js
// Mobile menu toggle
document
  .getElementById("mobileMenuToggle")
  .addEventListener("click", function () {
    document.getElementById("sidebar").classList.toggle("active");
    this.innerHTML = document
      .getElementById("sidebar")
      .classList.contains("active")
      ? '<i class="fas fa-times"></i>'
      : '<i class="fas fa-bars"></i>';
  });

/* -------------------------------------------
   TIMER + ETA SYSTEM (ADD THIS BLOCK)
------------------------------------------- */

// Track per-facility timers
let facilityTimers = {};
let startTime = null;
let totalTimerInterval = null;

// Start a timer for a single facility
function startFacilityTimer(index) {
  facilityTimers[index] = {
    start: Date.now(),
    interval: setInterval(() => updateFacilityElapsedTime(index), 1000),
  };
}

// Stop timer for facility
function stopFacilityTimer(index) {
  if (facilityTimers[index]) {
    clearInterval(facilityTimers[index].interval);
  }
}

// Update each facility’s timer display
function updateFacilityElapsedTime(index) {
  const timerEl = document.getElementById(`time-${index}`);
  if (!timerEl || !facilityTimers[index]) return;

  const elapsed = Math.floor((Date.now() - facilityTimers[index].start) / 1000);
  timerEl.textContent = formatTime(elapsed);
}

// Update total elapsed + ETA
function updateTotalElapsedTime() {
  if (!startTime) return;

  const now = Date.now();
  const elapsedSec = Math.floor((now - startTime) / 1000);

  // Update total elapsed
  const totalEl = document.getElementById("totalElapsed");
  if (totalEl) {
    totalEl.textContent = formatTime(elapsedSec);
  }

  // Count completed facilities
  const completed = document.querySelectorAll(
    ".facility-progress.complete"
  ).length;

  const total = uploadedData.length;

  if (completed === 0) {
    const etaEl = document.getElementById("eta");
    if (etaEl) etaEl.textContent = "Estimating…";
    return;
  }

  // ETA = average time per completed * remaining
  const avgPerItem = elapsedSec / completed;
  const remaining = Math.round(avgPerItem * (total - completed));

  const etaEl = document.getElementById("eta");
  if (etaEl) etaEl.textContent = formatTime(remaining);
}

function formatTime(sec) {
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}m ${s}s`;
}

// Initialize DataTable
$("#resultsTable thead").addClass("table-primary");

$(document).ready(function () {
  window.resultsTable = $("#resultsTable").DataTable({
    searchPanes: { cascadePanes: true, viewTotal: true },
    buttons: [
      {
        extend: "excelHtml5",
        text: '<i class="fas fa-file-excel me-1"></i> Export to Excel',
        className: "btn btn-success btn-sm",
      },
      {
        extend: "csvHtml5",
        text: '<i class="fas fa-file-csv me-1"></i> Export CSV',
        className: "btn btn-outline-primary btn-sm",
      },
    ],
    pageLength: 10,
    responsive: true,
    order: [],
    scrollY: "400px",
    scrollCollapse: true,
    scrollX: true,
    language: {
      search: "Search:",
      lengthMenu: "Show _MENU_ entries",
    },
    dom: "Bfrtip",
    autoWidth: false,
    columnDefs: [
      { width: "150px", targets: 0 },
      { width: "120px", targets: [1, 2] },
      { width: "100px", targets: [3, 4, 5] },
      { width: "90px", targets: [6, 7, 8] },
      { width: "110px", targets: [9, 10, 11] },
      { width: "100px", targets: [12, 13] },
    ],
  });
});

// --- Base layers ---
const streetLayer = L.tileLayer(
  "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
  {
    attribution: "&copy; OpenStreetMap contributors",
  }
);

const satelliteLayer = L.tileLayer(
  "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
  { attribution: "Tiles © Esri", maxZoom: 20 }
);

const map = L.map("map", {
  center: [-13.2543, 34.3015],
  zoom: 6,
  layers: [streetLayer],
  scrollWheelZoom: true,
});

const baseMaps = {
  "Street Map": streetLayer,
  Satellite: satelliteLayer,
};

L.control.layers(baseMaps).addTo(map);

let uploadedData = [];
let countryBoundaryLayer = null;
let markers = {};
let countryData = null;

const countryAlpha2Map = {
  MW: "mw",
  KE: "ke",
  TZ: "tz",
  UG: "ug",
  ZM: "zm",
  ZW: "zw",
  NG: "ng",
};

const API_CONFIG = {
  worldpop: "https://coordinates-checker-dc59.onrender.com/api/worldpop",
  nominatim: "https://coordinates-checker-dc59.onrender.com/api/nominatim",
  overture: "https://coordinates-checker-dc59.onrender.com/api/overture_match",
  road_distance:
    "https://coordinates-checker-dc59.onrender.com/api/road_distance",
  building_distance:
    "https://coordinates-checker-dc59.onrender.com/api/building_distance",
  water_check: "https://coordinates-checker-dc59.onrender.com/api/water_check",
};

document
  .getElementById("fileUpload")
  .addEventListener("change", handleFileUpload);
document.getElementById("countrySelect").addEventListener("change", () => {
  const country = document.getElementById("countrySelect").value;
  if (country) loadCountryBoundary(country);
  updateValidateButton();
});
document
  .getElementById("validateBtn")
  .addEventListener("click", validateCoordinates);

function handleFileUpload(e) {
  const file = e.target.files[0];
  if (!file) return;
  Papa.parse(file, {
    header: true,
    complete: function (results) {
      uploadedData = results.data.filter((r) => r.Name && r.x && r.y);
      updateValidateButton();
      updateDataSummary();
      updateStats();
    },
  });
}

function updateStats() {
  document.getElementById("totalCount").textContent = uploadedData.length;
  document.getElementById("validCount").textContent = "0";
  document.getElementById("warningCount").textContent = "0";
  document.getElementById("invalidCount").textContent = "0";
}

function updateValidateButton() {
  const country = document.getElementById("countrySelect").value;
  document.getElementById("validateBtn").disabled = !(
    country && uploadedData.length > 0
  );
}

function updateDataSummary() {
  const summaryDiv = document.getElementById("dataSummary");
  if (uploadedData.length === 0) {
    summaryDiv.innerHTML =
      '<div class="summary-card"><p class="text-muted">No data uploaded yet</p></div>';
    return;
  }

  const xVals = uploadedData
    .map((d) => parseFloat(d.x))
    .filter((val) => !isNaN(val));
  const yVals = uploadedData
    .map((d) => parseFloat(d.y))
    .filter((val) => !isNaN(val));

  summaryDiv.innerHTML = `
        <div class="summary-card">
          <p><strong>Facilities:</strong> ${uploadedData.length}</p>
          <p><strong>X range:</strong> ${
            xVals.length ? Math.min(...xVals).toFixed(4) : "N/A"
          } - ${xVals.length ? Math.max(...xVals).toFixed(4) : "N/A"}</p>
          <p><strong>Y range:</strong> ${
            yVals.length ? Math.min(...yVals).toFixed(4) : "N/A"
          } - ${yVals.length ? Math.max(...yVals).toFixed(4) : "N/A"}</p>
        </div>
      `;
}

async function loadCountryBoundary(countryCode) {
  updateApiStatus("Loading country boundary...", "info");
  try {
    const res = await fetch(
      `https://nominatim.openstreetmap.org/search?country=${countryCode}&polygon_geojson=1&format=json`
    );
    const data = await res.json();
    if (data && data[0] && data[0].geojson) {
      if (countryBoundaryLayer) map.removeLayer(countryBoundaryLayer);
      countryBoundaryLayer = L.geoJSON(data[0].geojson, {
        style: { color: "blue", weight: 2, fillOpacity: 0.1 },
      }).addTo(map);
      map.fitBounds(countryBoundaryLayer.getBounds());
      updateApiStatus("Country boundary loaded", "success");
    }
  } catch (e) {
    console.error(e);
    updateApiStatus("Could not load country boundary", "error");
  }
}

function normalize(str) {
  return str ? str.toLowerCase().replace(/\s+/g, "").trim() : "";
}

// Progress tracking UI functions
function createProgressCard(facility, index) {
  const card = document.createElement("div");
  card.className = "facility-progress processing";
  card.id = `progress-${index}`;
  card.innerHTML = `
        <div class="facility-header">
          <span class="facility-name">${facility.Name}</span>
          <span class="facility-status status-processing" id="status-${index}">Processing...</span>
        </div>
        <div class="facility-timer">
          <small>⏱ <span id="time-${index}">0s</span></small>
        </div>
        <div class="api-checks">
          <div class="api-check check-pending" id="check-country-${index}">
            <span>Country</span>
            <span>⏳</span>
          </div>
          <div class="api-check check-pending" id="check-admin-${index}">
            <span>Admin</span>
            <span>⏳</span>
          </div>
          <div class="api-check check-pending" id="check-road-${index}">
            <span>Road</span>
            <span>⏳</span>
          </div>
          <div class="api-check check-pending" id="check-building-${index}">
            <span>Building</span>
            <span>⏳</span>
          </div>
          <div class="api-check check-pending" id="check-water-${index}">
            <span>Water</span>
            <span>⏳</span>
          </div>
          <div class="api-check check-pending" id="check-population-${index}">
            <span>Population</span>
            <span>⏳</span>
          </div>
        </div>
      `;
  return card;
}

function updateCheckStatus(index, checkName, status) {
  const checkEl = document.getElementById(`check-${checkName}-${index}`);
  if (!checkEl) return;

  checkEl.className = `api-check check-${status}`;
  const icon =
    status === "loading"
      ? '<span class="spinner-icon"><i class="fas fa-spinner fa-spin"></i></span>'
      : status === "success"
      ? "✓"
      : status === "failed"
      ? "✗"
      : "⏳";
  checkEl.querySelector("span:last-child").innerHTML = icon;
}

function updateFacilityStatus(index, status, message = "") {
  const statusEl = document.getElementById(`status-${index}`);
  const cardEl = document.getElementById(`progress-${index}`);
  if (!statusEl || !cardEl) return;

  cardEl.className = `facility-progress ${status}`;
  statusEl.className = `facility-status status-${status}`;
  statusEl.textContent =
    message ||
    (status === "complete"
      ? "Complete"
      : status === "error"
      ? "Error"
      : "Processing...");
}

function updateOverallProgress(current, total) {
  const percent = Math.round((current / total) * 100);
  document.getElementById(
    "progressText"
  ).textContent = `${current} / ${total} facilities`;
  document.getElementById("progressPercent").textContent = `${percent}%`;
  document.getElementById("progressBar").style.width = `${percent}%`;
}

function isSimilar(a, b) {
  if (!a || !b) return false;
  a = a
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
  b = b
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim();
  return a === b || a.startsWith(b) || b.startsWith(a);
}

// --- Core validation with progress tracking ---
async function validateCoordinates() {
  startTime = Date.now();
  if (totalTimerInterval) clearInterval(totalTimerInterval);
  totalTimerInterval = setInterval(updateTotalElapsedTime, 1000);

  const country = document.getElementById("countrySelect").value;
  const results = [];
  const batchSize = 6;

  // Show progress section
  document.getElementById("progressSection").style.display = "block";
  document.getElementById("overallProgress").style.display = "block";
  const tracker = document.getElementById("progressTracker");
  tracker.innerHTML = "";

  // Create progress cards for all facilities
  uploadedData.forEach((f, i) => {
    tracker.appendChild(createProgressCard(f, i));
  });

  async function validateSingleFacility(f, index, total) {
    startFacilityTimer(index);
    const x = parseFloat(f.x),
      y = parseFloat(f.y);
    updateApiStatus(
      `Validating facility ${index + 1}/${total}: ${f.Name}`,
      "info"
    );

    // Country check
    updateCheckStatus(index, "country", "loading");
    try {
      const countryResp = await fetch(
        `${API_CONFIG.nominatim}?lat=${y}&lon=${x}&format=json&addressdetails=1`
      );
      countryData = await countryResp.json();
      const expectedCountry = countryAlpha2Map[country];
      f.countryBoundary = {
        valid: countryData.address?.country_code === expectedCountry,
        message: countryData.address?.country || "Unknown",
      };
      updateCheckStatus(
        index,
        "country",
        f.countryBoundary.valid ? "success" : "failed"
      );
    } catch (e) {
      f.countryBoundary = { valid: false, message: "Country check failed" };
      updateCheckStatus(index, "country", "failed");
    }

    // --- Admin1 check ---
    updateCheckStatus(index, "admin", "loading");
    try {
      const osmAdmin1 = normalize(countryData.address?.state || "");
      const admin1 = normalize(f.Admin1);
      const admin1Match = isSimilar(admin1, osmAdmin1);
      const score = admin1Match ? 100 : 0;
      const msg = admin1Match ? "Admin1 matches" : "Admin1 mismatch";

      f.adminAreaMatch = {
        valid: admin1Match,
        score,
        message: msg,
      };

      updateCheckStatus(index, "admin", admin1Match ? "success" : "failed");
    } catch (e) {
      console.error("Admin1 check failed", e);
      f.adminAreaMatch = {
        valid: false,
        score: 0,
        message: "Admin1 check failed",
      };
      updateCheckStatus(index, "admin", "failed");
    }

    // Duplicate check (instant, no API)
    const threshold = 0.001;
    const duplicates = uploadedData.filter(
      (o) =>
        o !== f &&
        Math.abs(parseFloat(o.x) - x) < threshold &&
        Math.abs(parseFloat(o.y) - y) < threshold
    );
    f.duplicateCheck = {
      valid: duplicates.length === 0,
      message:
        duplicates.length === 0
          ? "No duplicates"
          : `${duplicates.length} duplicates`,
    };

    // Road distance
    updateCheckStatus(index, "road", "loading");
    try {
      const roadResp = await fetch(
        `${API_CONFIG.road_distance}?lat=${y}&lon=${x}`
      );
      const roadData = await roadResp.json();
      f.roadDistance = roadData.valid
        ? {
            valid: true,
            distance: roadData.distance,
            message: roadData.message,
          }
        : {
            valid: false,
            distance: null,
            message: roadData.message || "No road nearby",
          };
      updateCheckStatus(index, "road", roadData.valid ? "success" : "failed");
    } catch (e) {
      f.roadDistance = {
        valid: false,
        distance: null,
        message: "Road distance failed",
      };
      updateCheckStatus(index, "road", "failed");
    }

    // Building distance
    updateCheckStatus(index, "building", "loading");
    try {
      const buildResp = await fetch(
        `${API_CONFIG.building_distance}?lat=${y}&lon=${x}`
      );
      const buildData = await buildResp.json();
      f.buildingDistance = buildData.valid
        ? {
            valid: true,
            distance: buildData.distance,
            message: buildData.message,
          }
        : {
            valid: false,
            distance: null,
            message: buildData.message || "No building nearby",
          };
      updateCheckStatus(
        index,
        "building",
        buildData.valid ? "success" : "failed"
      );
    } catch (e) {
      f.buildingDistance = {
        valid: false,
        distance: null,
        message: "Building distance failed",
      };
      updateCheckStatus(index, "building", "failed");
    }

    // Water check
    updateCheckStatus(index, "water", "loading");
    try {
      const waterResp = await fetch(
        `${API_CONFIG.water_check}?lat=${y}&lon=${x}`
      );
      const waterData = await waterResp.json();
      f.waterCheck = { valid: waterData.on_water };
      updateCheckStatus(
        index,
        "water",
        waterData.on_water ? "failed" : "success"
      );
    } catch (e) {
      f.waterCheck = {
        valid: false,
        distance: null,
        message: "Water check failed",
      };
      updateCheckStatus(index, "water", "failed");
    }

    // Population check
    updateCheckStatus(index, "population", "loading");
    try {
      const popResp = await fetch(
        `${API_CONFIG.worldpop}?latitude=${y}&longitude=${x}`
      );
      const popData = await popResp.json();
      const population = popData.population || 0;

      f.populationDensity = {
        valid: population > 100,
        population,
        message: `Population ~1km: ${population}`,
      };

      updateCheckStatus(
        index,
        "population",
        f.populationDensity.valid ? "success" : "failed"
      );
    } catch (e) {
      f.populationDensity = {
        valid: false,
        population: 0,
        message: "Population check failed",
      };
      updateCheckStatus(index, "population", "failed");
    }

    // Calculate overall score
    f.overallScore = Math.round(
      (f.countryBoundary.valid ? 1 : 0) * 30 +
        f.adminAreaMatch.score * 0.15 +
        (f.duplicateCheck.valid ? 1 : 0) * 20 +
        (f.roadDistance.valid ? 1 : 0) * 10 +
        (f.buildingDistance.valid ? 1 : 0) * 10 +
        (!f.waterCheck.on_water ? 1 : 0) * 30
    );

    updateFacilityStatus(index, "complete", `Score: ${f.overallScore}%`);
    stopFacilityTimer(index);

    return f;
  }

  // Process facilities in batches
  for (let i = 0; i < uploadedData.length; i += batchSize) {
    const batch = uploadedData.slice(i, i + batchSize);
    const batchResults = await Promise.all(
      batch.map((f, idx) =>
        validateSingleFacility(f, i + idx, uploadedData.length)
      )
    );
    results.push(...batchResults);
    updateOverallProgress(results.length, uploadedData.length);
  }

  updateApiStatus(
    `Validation complete: ${results.length} facilities processed`,
    "success"
  );
  // displayValidationResults(results);
  updateMap(results);
  updateResultsTable(results);
  updateStatsFromResults(results);
  // STOP TOTAL TIMER
  if (totalTimerInterval) {
    clearInterval(totalTimerInterval);
    totalTimerInterval = null;
  }
}

// function displayValidationResults(results) {
//   const div = document.getElementById('validationResults');
//   const valid = results.filter(r => r.overallScore >= 70).length;
//   const warn = results.filter(r => r.overallScore >= 50 && r.overallScore < 70).length;
//   const invalid = results.filter(r => r.overallScore < 50).length;

//   div.innerHTML = `
//     <div class="summary-card">
//       <div class="d-flex justify-content-between mb-2">
//         <span class="badge bg-success">Valid: ${valid}</span>
//         <span class="badge bg-warning">Warning: ${warn}</span>
//         <span class="badge bg-danger">Invalid: ${invalid}</span>
//       </div>
//       <p class="mb-0"><strong>Overall Score:</strong> ${results.length ? Math.round(results.reduce((sum, r) => sum + r.overallScore, 0) / results.length) : 0}%</p>
//     </div>
//   `;
// }

function updateStatsFromResults(results) {
  const valid = results.filter((r) => r.overallScore >= 70).length;
  const warn = results.filter(
    (r) => r.overallScore >= 50 && r.overallScore < 70
  ).length;
  const invalid = results.filter((r) => r.overallScore < 50).length;

  document.getElementById("validCount").textContent = valid;
  document.getElementById("warningCount").textContent = warn;
  document.getElementById("invalidCount").textContent = invalid;
  document.getElementById("totalCount").textContent = results.length;
}

function updateMap(results) {
  map.eachLayer((l) => {
    if (l instanceof L.Marker) map.removeLayer(l);
  });
  markers = {};

  results.forEach((f, i) => {
    const color =
      f.overallScore >= 70
        ? "#4cc9f0"
        : f.overallScore >= 50
        ? "#f72585"
        : "#ff0054";
    const m = L.marker([parseFloat(f.y), parseFloat(f.x)], {
      icon: L.divIcon({
        className: "custom-marker",
        html: `<div style="background-color:${color};width:16px;height:16px;border-radius:50%;border:3px solid white;box-shadow: 0 2px 5px rgba(0,0,0,0.3);"></div>`,
        iconSize: [22, 22],
        iconAnchor: [11, 11],
      }),
    }).addTo(map);

    m.bindPopup(`
          <div style="min-width: 200px;">
            <h6 style="margin: 0 0 10px 0; color: ${color}"><strong>${
      f.Name
    }</strong></h6>
            <p style="margin: 0 0 5px 0;"><strong>Coordinates:</strong> ${
              f.x
            }, ${f.y}</p>
            <p style="margin: 0 0 5px 0;"><strong>Score:</strong> ${
              f.overallScore
            }%</p>
            <p style="margin: 0 0 5px 0;"><strong>Country Match:</strong> ${
              f.countryBoundary.valid ? "✓" : "✗"
            }</p>
            <p style="margin: 0 0 5px 0;"><strong>On Water:</strong> ${
              f.waterCheck?.on_water ? "Yes" : "No"
            }</p>
          </div>
        `);

    markers[i] = m;
  });

  // Fit map to show all markers if there are any
  if (results.length > 0) {
    const markerGroup = new L.featureGroup(Object.values(markers));
    map.fitBounds(markerGroup.getBounds().pad(0.1));
  }
}

function updateResultsTable(results) {
  // Map results to DataTables-compatible array
  const data = results.map((f) => {
    // Determine row class based on overallScore
    const rowClass =
      f.overallScore >= 70
        ? "table-success"
        : f.overallScore >= 50
        ? "table-warning"
        : "table-danger";

    return {
      DT_RowClass: rowClass, // DataTables will apply this class to the row
      0: f.Name,
      1: f.x || "",
      2: f.y || "",
      3: f.Admin1 || "",
      4: f.Admin2 || "",
      5: f.Admin3 || "",
      6: `<span class="badge ${
        f.countryBoundary.valid ? "bg-success" : "bg-danger"
      }">${f.countryBoundary.valid ? "✓" : "✗"}</span>`,
      7: `<span class="badge ${
        f.adminAreaMatch.valid ? "bg-success" : "bg-warning"
      }">${f.adminAreaMatch.valid ? "✓" : "!"}</span>`,
      8: `<span class="badge ${
        f.duplicateCheck.valid ? "bg-success" : "bg-danger"
      }">${f.duplicateCheck.valid ? "✓" : "✗"}</span>`,
      9: `<span class="badge ${
        f.roadDistance.valid ? "bg-success" : "bg-warning"
      }">${
        f.roadDistance.distance >= 0
          ? f.roadDistance.distance.toFixed(0) + "m"
          : "N/A"
      }</span>`,
      10: `<span class="badge ${
        !f.waterCheck?.on_water ? "bg-success" : "bg-warning"
      }">${!f.waterCheck?.on_water ? "✓" : "✗"}</span>`,
      11: `<span class="badge ${
        f.buildingDistance.valid ? "bg-success" : "bg-warning"
      }">${
        f.buildingDistance.distance !== null
          ? f.buildingDistance.distance < 1
            ? "At location"
            : f.buildingDistance.distance.toFixed(0) + "m"
          : "N/A"
      }</span>`,
      12: `<span class="badge ${
        f.populationDensity.valid ? "bg-success" : "bg-warning"
      }">${
        f.populationDensity.population
          ? f.populationDensity.population.toLocaleString()
          : "N/A"
      }</span>`,
      13: `<span class="badge ${
        f.overallScore >= 70
          ? "bg-success"
          : f.overallScore >= 50
          ? "bg-warning"
          : "bg-danger"
      }">${f.overallScore}%</span>`,
    };
  });

  // Clear previous data and add new rows
  window.resultsTable.clear();
  window.resultsTable.rows.add(data);
  window.resultsTable.draw();

  // Row click to focus marker on map
  $("#resultsTable tbody")
    .off("click")
    .on("click", "tr", function () {
      const rowIndex = window.resultsTable.row(this).index();
      const marker = markers[rowIndex];
      if (marker) {
        map.setView(marker.getLatLng(), 16);
        marker.openPopup();
      }
    });
}

function updateApiStatus(msg, type = "info") {
  const statusEl = document.getElementById("apiStatus");
  statusEl.textContent = msg;
  statusEl.className = `api-status status-${type}`;
}

// Close mobile sidebar when clicking outside on mobile
document.addEventListener("click", function (event) {
  const sidebar = document.getElementById("sidebar");
  const toggleBtn = document.getElementById("mobileMenuToggle");
  const isClickInsideSidebar = sidebar.contains(event.target);
  const isClickOnToggle = toggleBtn.contains(event.target);

  if (
    window.innerWidth <= 992 &&
    sidebar.classList.contains("active") &&
    !isClickInsideSidebar &&
    !isClickOnToggle
  ) {
    sidebar.classList.remove("active");
    toggleBtn.innerHTML = '<i class="fas fa-bars"></i>';
  }
});
