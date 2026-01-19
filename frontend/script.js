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

// Update each facility's timer display
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

      {
        text: '<i class="fas fa-info-circle me-1"></i> Methodology',
        className: "btn btn-secondary btn-sm",
        action: function (e, dt, node, config) {
          showMethodology();
        },
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

L.control.fullscreen({ position: "topleft" }).addTo(map);
map.on("fullscreenchange", function () {
  map.invalidateSize();
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

const csvTemplateHeaders = ["Name", "x", "y", "Admin1", "Admin2", "Admin3"];

const API_CONFIG = {
  worldpop: " http://127.0.0.1:5000/api/worldpop",
  nominatim: " http://127.0.0.1:5000/api/nominatim",
  overture: " http://127.0.0.1:5000/api/overture_match",
  road_distance: " http://127.0.0.1:5000/api/road_distance",
  building_distance: " http://127.0.0.1:5000/api/building_distance",
  water_check: " http://127.0.0.1:5000/api/water_check",
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
  const validateBtn = document.getElementById("validateBtn");
  validateBtn.disabled = !(country && uploadedData.length > 0);

  // Enable/disable the download button based on uploaded data
  const downloadBtn = document.querySelector(".btn-info.btn-sm");
  if (downloadBtn) {
    downloadBtn.disabled = uploadedData.length === 0;
  }
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

// Scenario
document
  .getElementById("scenarioUpload")
  .addEventListener("change", handleScenarioUpload);

function handleScenarioUpload(e) {
  const file = e.target.files[0];
  if (!file) return;

  Papa.parse(file, {
    header: true,
    skipEmptyLines: true,
    complete: function (results) {
      const scenarioData = results.data;

      if (!scenarioData.length) {
        alert("Scenario file is empty or invalid.");
        return;
      }

      // Normalize scenario rows into system format
      const normalizedResults = scenarioData.map((row) => ({
        Name: row["Facility"],

        x: parseFloat(row["Longitude (X)"]),
        y: parseFloat(row["Latitude (Y)"]),

        Admin1: row["Admin1"] || "",
        Admin2: row["Admin2"] || "",
        Admin3: row["Admin3"] || "",

        // Final classification already decided
        category: row["Score"] || "Unknown",

        countryBoundary: {
          valid: row["Country"] === "Pass",
          countryName: row["Admin1"] || "",
        },

        adminAreaMatch: {
          valid: row["Admin Area match"] === "Pass",
          osmAdminName: row["OSM Admin Area 1"] || "",
        },

        duplicateCheck: {
          valid: row["Duplicate"] === "Pass",
        },

        roadDistance: {
          valid: row["Nearby Road (m)"] !== "",
          distance: row["Nearby Road (m)"]
            ? parseFloat(row["Nearby Road (m)"])
            : null,
          id: row["Overture road ID"] || null,
        },

        buildingDistance: {
          valid: row["Nearby Building (m)"] !== "",
          distance: row["Nearby Building (m)"]
            ? parseFloat(row["Nearby Building (m)"])
            : null,
          id: row["Overture building ID"] || null,
        },

        waterCheck: {
          on_water: row["On Water"] !== "Pass",
        },

        populationDensity: {
          valid: !!row["Population"],
          population: row["Population"]
            ? parseInt(row["Population"].replace(/,/g, ""))
            : null,
        },

        error: false,
      }));

      // Reset timers and progress UI
      if (totalTimerInterval) {
        clearInterval(totalTimerInterval);
        totalTimerInterval = null;
      }

      document.getElementById("progressSection").style.display = "none";
      document.getElementById("overallProgress").style.display = "none";

      // Populate system
      uploadedData = normalizedResults;
      updateMap(normalizedResults);
      zoomMapToResults(normalizedResults);

      updateResultsTable(normalizedResults);
      updateStatsFromResults(normalizedResults);

      updateApiStatus(
        `Scenario loaded: ${normalizedResults.length} facilities`,
        "success"
      );
    },
  });
}

function zoomMapToResults(results) {
  if (!results || results.length === 0) return;

  const latLngs = results
    .filter((r) => !isNaN(r.y) && !isNaN(r.x))
    .map((r) => [r.y, r.x]);

  if (latLngs.length === 0) return;

  const bounds = L.latLngBounds(latLngs);
  map.fitBounds(bounds, {
    padding: [40, 40],
    maxZoom: 12,
  });
}

//Csv template
document
  .getElementById("downloadTemplateBtn")
  .addEventListener("click", downloadCsvTemplate);

function downloadCsvTemplate() {
  const rows = [csvTemplateHeaders];

  const csvContent = rows.map((row) => row.join(",")).join("\n");

  const blob = new Blob([csvContent], {
    type: "text/csv;charset=utf-8;",
  });

  const url = URL.createObjectURL(blob);

  const link = document.createElement("a");
  link.href = url;
  link.download = "health_facility_upload_template.csv";

  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);

  URL.revokeObjectURL(url);
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

// Function to determine facility category
function determineFacilityCategory(f) {
  // Check for immediate fails (Invalid)
  if (!f.countryBoundary?.valid || f.waterCheck?.on_water) {
    return "Invalid";
  }

  // Check for Data Consistency Review
  if (!f.duplicateCheck?.valid || !f.adminAreaMatch?.valid) {
    return "Data Consistency Review";
  }

  // Check for Location Accuracy Flags
  if (
    !f.roadDistance?.valid ||
    !f.buildingDistance?.valid ||
    !f.populationDensity?.valid
  ) {
    return "Location Accuracy Flags";
  }

  // If all checks pass
  return "Valid";
}

// --- Core validation with progress tracking ---
async function validateCoordinates() {
  startTime = Date.now();
  if (totalTimerInterval) clearInterval(totalTimerInterval);
  totalTimerInterval = setInterval(updateTotalElapsedTime, 1000);

  const country = document.getElementById("countrySelect").value;
  const results = [];
  const batchSize = 10;

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

    try {
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
          countryCode: countryData.address?.country_code || "",
          countryName: countryData.address?.country || "",
        };
        updateCheckStatus(
          index,
          "country",
          f.countryBoundary.valid ? "success" : "failed"
        );
      } catch (e) {
        f.countryBoundary = {
          valid: false,
          message: "Country check failed",
          countryCode: "",
          countryName: "",
        };
        updateCheckStatus(index, "country", "failed");
      }

      // Update the Admin1 check section in the validateSingleFacility function:

      // --- Admin1 check ---
      updateCheckStatus(index, "admin", "loading");
      try {
        // Extract admin1 name from Nominatim response - handle different field names
        const address = countryData?.address || {};

        // Try different possible field names for Admin1 level
        const osmAdmin1 =
          address?.region || // Alternative
          address?.state || // Most common
          address?.province || // Some countries use province
          address?.department || // Some countries use department
          address?.county || // Some countries use county
          address["ISO3166-2-lvl3"]?.split("-")[1] || // ISO code
          address["ISO3166-2-lvl4"]?.split("-")[1] || // Alternative ISO code
          "";

        const osmAdmin1Normalized = normalize(osmAdmin1);
        const admin1 = normalize(f.Admin1 || "");
        const admin1Match = isSimilar(admin1, osmAdmin1Normalized);
        const msg = admin1Match ? "Admin1 matches" : "Admin1 mismatch";

        f.adminAreaMatch = {
          valid: admin1Match,
          message: msg,
          osmAdminName: osmAdmin1, // Store the actual name from server
          uploadedAdminName: f.Admin1 || "", // Store the uploaded name
          matchStatus: admin1Match ? "match" : "mismatch",
          addressFields: address, // Store all address fields for debugging
        };

        updateCheckStatus(index, "admin", admin1Match ? "success" : "failed");
      } catch (e) {
        console.error("Admin1 check failed", e);
        f.adminAreaMatch = {
          valid: false,
          message: "Admin1 check failed",
          osmAdminName: "",
          uploadedAdminName: f.Admin1 || "",
          matchStatus: "error",
        };
        updateCheckStatus(index, "admin", "failed");
      }

      // Duplicate check (instant, no API)
      const threshold = 0.001;
      const duplicates = uploadedData.filter(
        (o, i) =>
          i !== index &&
          Math.abs(parseFloat(o.x) - x) < threshold &&
          Math.abs(parseFloat(o.y) - y) < threshold
      );
      f.duplicateCheck = {
        valid: duplicates.length === 0,
        message:
          duplicates.length === 0
            ? "No duplicates"
            : `${duplicates.length} duplicates`,
        duplicateCount: duplicates.length,
      };

      // Road distance
      updateCheckStatus(index, "road", "loading");
      try {
        const roadResp = await fetch(
          `${API_CONFIG.road_distance}?lat=${y}&lon=${x}`
        );
        const roadData = await roadResp.json();
        console.log("road", roadData);
        f.roadDistance = {
          valid: roadData.valid || false,
          id: roadData.id || null,
          distance: roadData.distance !== undefined ? roadData.distance : null,
          message: roadData.message || "No road nearby",
        };
        updateCheckStatus(index, "road", roadData.valid ? "success" : "failed");
      } catch (e) {
        f.roadDistance = {
          valid: false,
          distance: null,
          id: null,
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
        f.buildingDistance = {
          valid: buildData.valid || false,
          id: buildData.id || null,
          distance:
            buildData.distance !== undefined ? buildData.distance : null,
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
          id: null,
          distance: null,
          message: "Building distance failed",
        };
        updateCheckStatus(index, "building", "failed");
      }

      // Water check - FIXED: Store both valid and on_water properties
      updateCheckStatus(index, "water", "loading");
      try {
        const waterResp = await fetch(
          `${API_CONFIG.water_check}?lat=${y}&lon=${x}`
        );
        const waterData = await waterResp.json();
        const onWater = waterData.on_water || false;
        f.waterCheck = {
          on_water: onWater,
          message: onWater ? "On water body" : "Not on water",
        };
        updateCheckStatus(index, "water", !onWater ? "success" : "failed");
      } catch (e) {
        f.waterCheck = {
          on_water: false,
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
          valid: population > 0,
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

      // Determine category instead of score
      f.category = determineFacilityCategory(f);

      // Set status message based on category
      let statusMessage = "";
      switch (f.category) {
        case "Invalid":
          statusMessage = "Invalid (Country/Water issue)";
          break;
        case "Data Consistency Review":
          statusMessage = "Data Consistency Review";
          break;
        case "Location Accuracy Flags":
          statusMessage = "Location Accuracy Flags";
          break;
        case "Valid":
          statusMessage = "Valid";
          break;
        default:
          statusMessage = f.category;
      }

      updateFacilityStatus(index, "complete", statusMessage);
      stopFacilityTimer(index);
      return f;
    } catch (error) {
      console.error(`Error validating facility ${index}:`, error);
      updateFacilityStatus(index, "error", "Validation failed");
      stopFacilityTimer(index);

      // Return facility with error flag
      f.error = true;
      f.category = "Error";
      return f;
    }
  }

  // Process facilities in batches with error handling
  for (let i = 0; i < uploadedData.length; i += batchSize) {
    const batch = uploadedData.slice(i, i + batchSize);

    try {
      // Use Promise.allSettled instead of Promise.all for better error handling
      const batchPromises = batch.map((f, idx) =>
        validateSingleFacility(f, i + idx, uploadedData.length)
      );

      const batchResults = await Promise.allSettled(batchPromises);

      // Process both successful and failed promises
      batchResults.forEach((result, idx) => {
        if (result.status === "fulfilled") {
          results.push(result.value);
        } else {
          // Handle failed promise
          console.error(`Facility ${i + idx} failed:`, result.reason);
          const failedFacility = batch[idx];
          failedFacility.error = true;
          failedFacility.category = "Error";
          results.push(failedFacility);
        }
      });

      updateOverallProgress(results.length, uploadedData.length);
    } catch (batchError) {
      console.error("Batch processing error:", batchError);
      // Even if batch fails, mark all facilities in batch as errors
      batch.forEach((f, idx) => {
        f.error = true;
        f.category = "Error";
        results.push(f);
      });
      updateOverallProgress(results.length, uploadedData.length);
    }
  }

  updateApiStatus(
    `Validation complete: ${results.length} facilities processed`,
    "success"
  );

  // Update UI with results
  updateMap(results.filter((f) => !f.error)); // Only show non-error facilities on map
  updateResultsTable(results);
  updateStatsFromResults(results);

  // STOP TOTAL TIMER
  if (totalTimerInterval) {
    clearInterval(totalTimerInterval);
    totalTimerInterval = null;
  }
}

function updateStatsFromResults(results) {
  // Count by category
  const validCount = results.filter((r) => r.category === "Valid").length;
  const dataConsistencyCount = results.filter(
    (r) => r.category === "Data Consistency Review"
  ).length;
  const locationAccuracyCount = results.filter(
    (r) => r.category === "Location Accuracy Flags"
  ).length;
  const invalidCount = results.filter((r) => r.category === "Invalid").length;
  const errorCount = results.filter((r) => r.category === "Error").length;

  document.getElementById("validCount").textContent = validCount;
  document.getElementById("warningCount").textContent =
    dataConsistencyCount + locationAccuracyCount;
  document.getElementById("invalidCount").textContent =
    invalidCount + errorCount;
  document.getElementById("totalCount").textContent = results.length;
}

function updateMap(results) {
  // Clear previous marker layers
  if (window.markerCluster) {
    map.removeLayer(window.markerCluster);
  }

  // Create cluster group with chunked loading (smooth UI)
  const markerCluster = L.markerClusterGroup({
    chunkedLoading: true,
    spiderfyOnMaxZoom: true,
    disableClusteringAtZoom: 17,
  });

  window.markerCluster = markerCluster;

  // Canvas renderer (huge performance boost)
  const renderer = L.canvas({ padding: 0.5 });

  markers = {};

  results.forEach((f, i) => {
    if (f.error) return;

    // Assign marker color by category
    let color = "";
    switch (f.category) {
      case "Valid":
        color = "#198754";
        break;
      case "Data Consistency Review":
        color = "#ffa500";
        break;
      case "Location Accuracy Flags":
        color = "#ffa500";
        break;
      case "Invalid":
        color = "#ff0054";
        break;
      default:
        color = "#808080";
    }

    // Use fast CircleMarker + Canvas renderer
    const marker = L.circleMarker([parseFloat(f.y), parseFloat(f.x)], {
      renderer,
      radius: 6,
      weight: 2,
      color: "white",
      fillColor: color,
      fillOpacity: 0.95,
    });

    // Keep your popup content fully intact
    marker.bindPopup(`
      <div style="min-width: 250px;">
        <h6 style="margin: 0 0 10px 0; color: ${color}"><strong>${
      f.Name
    }</strong></h6>
        <p><strong>Coordinates:</strong> ${f.x}, ${f.y}</p>
        <p><strong>Status:</strong> ${f.category}</p>
        <p><strong>Country Match:</strong> ${
          f.countryBoundary?.valid ? "✓" : "✗"
        } (${f.countryBoundary?.countryName || "Unknown"})</p>
        <p><strong>On Water:</strong> ${
          f.waterCheck?.on_water ? "Yes" : "No"
        }</p>
        <p><strong>Duplicate:</strong> ${
          f.duplicateCheck?.valid ? "No" : "Yes"
        }</p>
        <p><strong>Road Distance:</strong> ${
          f.roadDistance?.distance !== null
            ? f.roadDistance.distance.toFixed(2)
            : "N/A"
        }</p>
        <p><strong>Building Distance:</strong> ${
          f.buildingDistance?.distance !== null
            ? f.buildingDistance.distance.toFixed(2) < 1
              ? "At location"
              : f.buildingDistance.distance.toFixed(2)
            : "N/A"
        }</p>
        <p>
          <strong>Admin Match:</strong> ${
            f.adminAreaMatch?.valid ? "✓" : "✗"
          }<br>
          <small>Uploaded: ${f.Admin1 || "N/A"}</small><br>
          <small>OSM: ${f.adminAreaMatch?.osmAdminName || "N/A"}</small>
        </p>
      </div>
    `);

    markers[i] = marker;
    markerCluster.addLayer(marker);
  });

  // Add clusters to map
  map.addLayer(markerCluster);

  // Fit map to markers
  if (Object.values(markers).length > 0) {
    const bounds = markerCluster.getBounds();
    if (bounds.isValid()) map.fitBounds(bounds.pad(0.1));
  }
}

function updateResultsTable(results) {
  // Map results to DataTables-compatible array
  const data = results.map((f, index) => {
    // Determine row class based on category
    let rowClass = "";
    switch (f.category) {
      case "Valid":
        rowClass = "table-success";
        break;
      case "Data Consistency Review":
        rowClass = "table-warning";
        break;
      case "Location Accuracy Flags":
        rowClass = "table-warning";
        break;
      case "Invalid":
        rowClass = "table-danger";
        break;
      case "Error":
        rowClass = "table-secondary";
        break;
      default:
        rowClass = "";
    }

    return {
      DT_RowClass: rowClass,
      0: f.Name || "Unknown",
      1: f.x || "",
      2: f.y || "",
      3: f.Admin1 || "",
      4: f.Admin2 || "",
      5: f.Admin3 || "",
      6: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            f.countryBoundary?.valid ? "bg-success" : "bg-danger"
          }">${f.countryBoundary?.valid ? "Pass" : "Fail"}</span>`,
      7: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<div>${f.adminAreaMatch?.osmAdminName || "N/A"}</small></div>`,
      8: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<div class="admin-comparison">   
             <div><span class="badge ${
               f.adminAreaMatch?.valid ? "bg-success" : "bg-warning"
             }">${f.adminAreaMatch?.valid ? "Pass" : "Fail"}</span></div>
           </div>`,
      9: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            f.duplicateCheck?.valid ? "bg-success" : "bg-warning"
          }">${f.duplicateCheck?.valid ? "Pass" : "Fail"}</span>`,
      10: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            f.roadDistance?.valid ? "bg-success" : "bg-warning"
          }">${
            f.roadDistance?.distance !== null
              ? f.roadDistance?.distance.toFixed(2)
              : "N/A"
          }</span>`,
      11: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<div>${f.roadDistance?.id || "N/A"}</small></div>`,
      12: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            !f.waterCheck?.on_water ? "bg-success" : "bg-danger"
          }">${!f.waterCheck?.on_water ? "Pass" : "Fail"}</span>`,
      13: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            f.buildingDistance?.valid ? "bg-success" : "bg-warning"
          }">${
            f.buildingDistance?.distance !== undefined &&
            f.buildingDistance?.distance !== null
              ? f.buildingDistance?.distance.toFixed(2)
              : "N/A"
          }</span>`,
      14: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<div>${f.buildingDistance?.id || "N/A"}</small></div>`,
      15: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            f.populationDensity?.valid ? "bg-success" : "bg-warning"
          }">${
            f.populationDensity?.population
              ? f.populationDensity.population.toLocaleString()
              : "N/A"
          }</span>`,
      16: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            f.category === "Valid"
              ? "bg-success"
              : f.category === "Invalid"
              ? "bg-danger"
              : "bg-warning"
          }">${f.category}</span>`,
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

// Show methodology modal with updated categorical system
function showMethodology() {
  // Create methodology modal content
  const methodologyContent = `
    <div class="modal fade" id="methodologyModal" tabindex="-1" aria-labelledby="methodologyModalLabel" aria-hidden="true">
      <div class="modal-dialog modal-lg">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title" id="methodologyModalLabel">
              <i class="fas fa-info-circle me-2"></i>Validation Methodology
            </h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
          </div>
          <div class="modal-body">
            <div class="methodology-section">
              <h6><i class="fas fa-globe-americas me-2"></i>Country Boundary Check</h6>
              <p>Uses Nominatim reverse geocoding to verify coordinates are within the selected country.</p>
              <ul>
                <li><strong>Source:</strong> OpenStreetMap Nominatim API</li>
                <li><strong>Impact:</strong> Immediate fail if incorrect</li>
                <li><strong>Validation:</strong> Compares country code from coordinates with selected country</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-water me-2"></i>Water Body Check</h6>
              <p>Checks if coordinates fall on water bodies (rivers, lakes, oceans).</p>
              <ul>
                <li><strong>Source:</strong> Overpass API water features query</li>
                <li><strong>Radius:</strong> 50 meters search radius</li>
                <li><strong>Impact:</strong> Immediate fail if on water</li>
                <li><strong>Validation:</strong> Points should not be on water bodies</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-map-pin me-2"></i>Administrative Area Match</h6>
              <p>Compares Admin1 level from uploaded data with OSM administrative boundaries.</p>
              <ul>
                <li><strong>Source:</strong> OpenStreetMap Nominatim API</li>
                <li><strong>Impact:</strong> Data Consistency Review if mismatch</li>
                <li><strong>Method:</strong> Fuzzy matching of Admin1 names after normalization</li>
                <li><strong>Display:</strong> Shows both uploaded name and OSM name for comparison</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-copy me-2"></i>Duplicate Check</h6>
              <p>Identifies coordinates that are very close to each other (within 0.001 degrees).</p>
              <ul>
                <li><strong>Threshold:</strong> 0.001 degrees (~111 meters)</li>
                <li><strong>Impact:</strong> Data Consistency Review if duplicates found</li>
                <li><strong>Purpose:</strong> Prevents duplicate facility entries</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-road me-2"></i>Road Distance</h6>
              <p>Calculates distance to nearest road using Overture Maps transportation data with Overpass API fallback.</p>
              <ul>
                <li><strong>Primary Source:</strong> Overture Maps (DuckDB query)</li>
                <li><strong>Fallback:</strong> Overpass API for road networks</li>
                <li><strong>Impact:</strong> Location Accuracy Flags if no road within 500m</li>
                <li><strong>Optimal:</strong> Distance ≤ 100 meters</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-building me-2"></i>Building Distance</h6>
              <p>Finds distance to nearest building using Overture Maps building data with Overpass API fallback.</p>
              <ul>
                <li><strong>Primary Source:</strong> Overture Maps (DuckDB query)</li>
                <li><strong>Fallback:</strong> Overpass API for building data</li>
                <li><strong>Impact:</strong> Location Accuracy Flags if no building within 200m</li>
                <li><strong>Optimal:</strong> Distance ≤ 50 meters</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-users me-2"></i>Population Density</h6>
              <p>Estimates population within ~1km radius using WorldPop dataset.</p>
              <ul>
                <li><strong>Source:</strong> WorldPop Global High Resolution Population (2020)</li>
                <li><strong>Resolution:</strong> 100m × 100m grid</li>
                <li><strong>Impact:</strong> Location Accuracy Flags if population ≤ 100</li>
                <li><strong>Validation:</strong> Minimum 100 people within ~1km radius</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-list-check me-2"></i>Categorical Validation System</h6>
              <p>Facilities are categorized based on validation results:</p>
              <table class="table table-sm">
                <thead>
                  <tr>
                    <th>Category</th>
                    <th>Conditions</th>
                    <th>Action Required</th>
                  </tr>
                </thead>
                <tbody>
                  <tr class="table-danger">
                    <td><strong>Invalid</strong></td>
                    <td>Wrong country OR on water body</td>
                    <td>Immediate correction needed</td>
                  </tr>
                  <tr class="table-warning">
                    <td><strong>Data Consistency Review</strong></td>
                    <td>Duplicate coordinates OR Admin1 mismatch</td>
                    <td>Review data consistency and administrative boundaries</td>
                  </tr>
                  <tr class="table-warning">
                    <td><strong>Location Accuracy Flags</strong></td>
                    <td>No road nearby OR No building nearby OR Low population</td>
                    <td>Verify coordinate accuracy and physical plausibility</td>
                  </tr>
                  <tr class="table-success">
                    <td><strong>Valid</strong></td>
                    <td>All checks pass</td>
                    <td>No action required</td>
                  </tr>
                </tbody>
              </table>
              <p class="mt-2">
                <strong>Category Interpretation:</strong><br>
                • <span class="text-success">Valid</span>: All checks pass - coordinates are reliable<br>
                • <span class="text-warning">Data Consistency Review</span>: Potential data entry or administrative issues<br>
                • <span class="text-warning">Location Accuracy Flags</span>: Physical location may be inaccurate<br>
                • <span class="text-danger">Invalid</span>: Fundamental errors requiring immediate correction
              </p>
            </div>
            
            <div class="alert alert-info mt-3">
              <i class="fas fa-lightbulb me-2"></i>
              <strong>Note:</strong> All API calls are cached for performance. Processing time depends on number of facilities and API availability.
            </div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
          </div>
        </div>
      </div>
    </div>
  `;

  // Add modal to DOM if not already present
  let modal = document.getElementById("methodologyModal");
  if (!modal) {
    document.body.insertAdjacentHTML("beforeend", methodologyContent);
    modal = document.getElementById("methodologyModal");
  }

  // Show the modal
  const bootstrapModal = new bootstrap.Modal(modal);
  bootstrapModal.show();
}

// Add CSS for methodology modal styling and admin comparison
const methodologyCSS = `
.methodology-section {
  margin-bottom: 1.5rem;
  padding-bottom: 1rem;
  border-bottom: 1px solid #eee;
}

.methodology-section:last-child {
  border-bottom: none;
}

.methodology-section h6 {
  color: #2c3e50;
  font-weight: 600;
  margin-bottom: 0.5rem;
}

.methodology-section ul {
  margin-bottom: 0.5rem;
  padding-left: 1.5rem;
}

.methodology-section li {
  margin-bottom: 0.25rem;
}

.admin-comparison {
  font-size: 0.85rem;
  line-height: 1.3;
}

.admin-comparison div {
  margin-bottom: 2px;
}

.admin-comparison small {
  display: block;
}
`;

// Inject CSS
if (!document.getElementById("methodology-css")) {
  const style = document.createElement("style");
  style.id = "methodology-css";
  style.textContent = methodologyCSS;
  document.head.appendChild(style);
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
