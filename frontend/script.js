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

// Track ALL failed checks (both validation failures and API failures)
let failedChecks = {};
let isRetrying = false;

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
        text: '<i class="fas fa-download me-1"></i> Download Dataset',
        className: "btn btn-info btn-sm",
        action: function (e, dt, node, config) {
          downloadOriginalDataset();
        },
      },
      {
        text: '<i class="fas fa-redo me-1"></i> Retry Failed',
        className: "btn btn-warning btn-sm",
        action: function (e, dt, node, config) {
          retryFailedChecks();
        },
        attr: {
          id: "retryBtn",
        },
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

function updateCheckStatus(index, checkName, status, isApiError = false) {
  const checkEl = document.getElementById(`check-${checkName}-${index}`);
  if (!checkEl) return;

  checkEl.className = `api-check check-${status}`;
  if (isApiError) {
    checkEl.classList.add("api-error");
  }

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
      : status === "retrying"
      ? "Retrying..."
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

// Function to add failed check to tracking (both validation failures and API failures)
function addFailedCheck(
  facilityIndex,
  checkType,
  facility,
  failureType = "validation"
) {
  if (!failedChecks[facilityIndex]) {
    failedChecks[facilityIndex] = {};
  }

  if (!failedChecks[facilityIndex][checkType]) {
    failedChecks[facilityIndex][checkType] = {
      facility: facility,
      retryCount: 0,
      maxRetries: 3,
      failureType: failureType, // 'validation' or 'api'
    };
    updateRetryButton();
  }
}

// Function to remove failed check from tracking
function removeFailedCheck(facilityIndex, checkType) {
  if (failedChecks[facilityIndex] && failedChecks[facilityIndex][checkType]) {
    delete failedChecks[facilityIndex][checkType];

    // If no more failed checks for this facility, remove the facility entry
    if (Object.keys(failedChecks[facilityIndex]).length === 0) {
      delete failedChecks[facilityIndex];
    }

    updateRetryButton();
  }
}

// Function to update retry button state
function updateRetryButton() {
  const retryBtn = document.getElementById("retryBtn");
  const totalFailedChecks = countFailedChecks();

  if (retryBtn) {
    if (totalFailedChecks > 0) {
      retryBtn.disabled = false;
      retryBtn.innerHTML = `<i class="fas fa-redo me-1"></i> Retry Failed (${totalFailedChecks})`;
    } else {
      retryBtn.disabled = true;
      retryBtn.innerHTML = `<i class="fas fa-redo me-1"></i> Retry Failed`;
    }
  }
}

// Count total failed checks
function countFailedChecks() {
  let total = 0;
  for (const facilityIndex in failedChecks) {
    total += Object.keys(failedChecks[facilityIndex]).length;
  }
  return total;
}

// Generic API call with retry logic
async function makeApiCall(url, facilityIndex, checkType, facility) {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error(
      `API call failed for ${checkType} check (facility ${facilityIndex}):`,
      error
    );

    // Add to failed checks tracking as API failure
    addFailedCheck(facilityIndex, checkType, facility, "api");

    throw error;
  }
}

// Check if a validation result is considered a "failure" for retry purposes
function isValidationFailure(checkType, result) {
  switch (checkType) {
    case "country":
      return !result.valid; // Wrong country
    case "admin":
      return !result.valid; // Admin mismatch
    case "road":
      return !result.valid; // No road nearby
    case "building":
      return !result.valid; // No building nearby
    case "water":
      return result.on_water; // On water
    case "population":
      return !result.valid; // Low population
    default:
      return false;
  }
}

// Function to run a single API check and track failures
async function runApiCheck(
  facility,
  facilityIndex,
  checkType,
  isRetry = false
) {
  const x = parseFloat(facility.x);
  const y = parseFloat(facility.y);
  let apiErrorOccurred = false;

  try {
    switch (checkType) {
      case "country":
        updateCheckStatus(facilityIndex, checkType, "loading");
        try {
          const countryData = await makeApiCall(
            `${API_CONFIG.nominatim}?lat=${y}&lon=${x}&format=json&addressdetails=1`,
            facilityIndex,
            checkType,
            facility
          );

          const country = document.getElementById("countrySelect").value;
          const expectedCountry = countryAlpha2Map[country];
          facility.countryBoundary = {
            valid: countryData.address?.country_code === expectedCountry,
            message: countryData.address?.country || "Unknown",
            countryCode: countryData.address?.country_code || "",
            countryName: countryData.address?.country || "",
          };

          // Store country data for admin check
          facility._countryData = countryData;

          // Track validation failure if country doesn't match
          if (isValidationFailure(checkType, facility.countryBoundary)) {
            addFailedCheck(facilityIndex, checkType, facility, "validation");
          }

          updateCheckStatus(
            facilityIndex,
            checkType,
            facility.countryBoundary.valid ? "success" : "failed"
          );
        } catch (error) {
          apiErrorOccurred = true;
          facility.countryBoundary = {
            valid: false,
            message: "API Error",
            countryCode: "",
            countryName: "",
          };
          updateCheckStatus(facilityIndex, checkType, "failed", true);
        }
        break;

      case "admin":
        updateCheckStatus(facilityIndex, checkType, "loading");
        try {
          // First ensure we have country data
          if (!facility._countryData) {
            // If country check failed, admin check will also fail
            facility.adminAreaMatch = {
              valid: false,
              message: "Country data missing",
              osmAdminName: "",
              uploadedAdminName: facility.Admin1 || "",
              matchStatus: "error",
            };
            updateCheckStatus(facilityIndex, checkType, "failed", true);
            addFailedCheck(facilityIndex, checkType, facility, "api");
            break;
          }

          const address = facility._countryData.address || {};
          const osmAdmin1 =
            address?.region ||
            address?.state ||
            address?.province ||
            address?.department ||
            address?.county ||
            address["ISO3166-2-lvl3"]?.split("-")[1] ||
            address["ISO3166-2-lvl4"]?.split("-")[1] ||
            "";

          const osmAdmin1Normalized = normalize(osmAdmin1);
          const admin1 = normalize(facility.Admin1 || "");
          const admin1Match = isSimilar(admin1, osmAdmin1Normalized);

          facility.adminAreaMatch = {
            valid: admin1Match,
            message: admin1Match ? "Admin1 matches" : "Admin1 mismatch",
            osmAdminName: osmAdmin1,
            uploadedAdminName: facility.Admin1 || "",
            matchStatus: admin1Match ? "match" : "mismatch",
            addressFields: address,
          };

          // Track validation failure if admin doesn't match
          if (isValidationFailure(checkType, facility.adminAreaMatch)) {
            addFailedCheck(facilityIndex, checkType, facility, "validation");
          }

          updateCheckStatus(
            facilityIndex,
            checkType,
            admin1Match ? "success" : "failed"
          );
        } catch (error) {
          apiErrorOccurred = true;
          facility.adminAreaMatch = {
            valid: false,
            message: "API Error",
            osmAdminName: "",
            uploadedAdminName: facility.Admin1 || "",
            matchStatus: "error",
          };
          updateCheckStatus(facilityIndex, checkType, "failed", true);
        }
        break;

      case "road":
        updateCheckStatus(facilityIndex, checkType, "loading");
        try {
          const roadData = await makeApiCall(
            `${API_CONFIG.road_distance}?lat=${y}&lon=${x}`,
            facilityIndex,
            checkType,
            facility
          );

          facility.roadDistance = {
            valid: roadData.valid || false,
            distance:
              roadData.distance !== undefined ? roadData.distance : null,
            message: roadData.message || "No road nearby",
          };

          // Track validation failure if no road nearby
          if (isValidationFailure(checkType, facility.roadDistance)) {
            addFailedCheck(facilityIndex, checkType, facility, "validation");
          }

          updateCheckStatus(
            facilityIndex,
            checkType,
            roadData.valid ? "success" : "failed"
          );
        } catch (error) {
          apiErrorOccurred = true;
          facility.roadDistance = {
            valid: false,
            distance: null,
            message: "API Error",
          };
          updateCheckStatus(facilityIndex, checkType, "failed", true);
        }
        break;

      case "building":
        updateCheckStatus(facilityIndex, checkType, "loading");
        try {
          const buildData = await makeApiCall(
            `${API_CONFIG.building_distance}?lat=${y}&lon=${x}`,
            facilityIndex,
            checkType,
            facility
          );

          facility.buildingDistance = {
            valid: buildData.valid || false,
            distance:
              buildData.distance !== undefined ? buildData.distance : null,
            message: buildData.message || "No building nearby",
          };

          // Track validation failure if no building nearby
          if (isValidationFailure(checkType, facility.buildingDistance)) {
            addFailedCheck(facilityIndex, checkType, facility, "validation");
          }

          updateCheckStatus(
            facilityIndex,
            checkType,
            buildData.valid ? "success" : "failed"
          );
        } catch (error) {
          apiErrorOccurred = true;
          facility.buildingDistance = {
            valid: false,
            distance: null,
            message: "API Error",
          };
          updateCheckStatus(facilityIndex, checkType, "failed", true);
        }
        break;

      case "water":
        updateCheckStatus(facilityIndex, checkType, "loading");
        try {
          const waterData = await makeApiCall(
            `${API_CONFIG.water_check}?lat=${y}&lon=${x}`,
            facilityIndex,
            checkType,
            facility
          );

          const onWater = waterData.on_water || false;
          facility.waterCheck = {
            on_water: onWater,
            message: onWater ? "On water body" : "Not on water",
          };

          // Track validation failure if on water
          if (isValidationFailure(checkType, facility.waterCheck)) {
            addFailedCheck(facilityIndex, checkType, facility, "validation");
          }

          updateCheckStatus(
            facilityIndex,
            checkType,
            !onWater ? "success" : "failed"
          );
        } catch (error) {
          apiErrorOccurred = true;
          facility.waterCheck = {
            on_water: false,
            message: "API Error",
          };
          updateCheckStatus(facilityIndex, checkType, "failed", true);
        }
        break;

      case "population":
        updateCheckStatus(facilityIndex, checkType, "loading");
        try {
          const popData = await makeApiCall(
            `${API_CONFIG.worldpop}?latitude=${y}&longitude=${x}`,
            facilityIndex,
            checkType,
            facility
          );

          const population = popData.population || 0;
          facility.populationDensity = {
            valid: population > 100,
            population,
            message: `Population ~1km: ${population}`,
          };

          // Track validation failure if low population
          if (isValidationFailure(checkType, facility.populationDensity)) {
            addFailedCheck(facilityIndex, checkType, facility, "validation");
          }

          updateCheckStatus(
            facilityIndex,
            checkType,
            facility.populationDensity.valid ? "success" : "failed"
          );
        } catch (error) {
          apiErrorOccurred = true;
          facility.populationDensity = {
            valid: false,
            population: 0,
            message: "API Error",
          };
          updateCheckStatus(facilityIndex, checkType, "failed", true);
        }
        break;
    }

    // If this was a retry and succeeded, remove from failed checks
    if (isRetry && !apiErrorOccurred) {
      // Check if the validation is now successful
      let isNowSuccessful = false;

      switch (checkType) {
        case "country":
          isNowSuccessful = facility.countryBoundary?.valid;
          break;
        case "admin":
          isNowSuccessful = facility.adminAreaMatch?.valid;
          break;
        case "road":
          isNowSuccessful = facility.roadDistance?.valid;
          break;
        case "building":
          isNowSuccessful = facility.buildingDistance?.valid;
          break;
        case "water":
          isNowSuccessful = !facility.waterCheck?.on_water;
          break;
        case "population":
          isNowSuccessful = facility.populationDensity?.valid;
          break;
      }

      if (
        isNowSuccessful &&
        failedChecks[facilityIndex] &&
        failedChecks[facilityIndex][checkType]
      ) {
        removeFailedCheck(facilityIndex, checkType);
      }
    }
  } catch (error) {
    console.error(
      `Error in ${checkType} check for facility ${facilityIndex}:`,
      error
    );
  }
}

// Main retry function for failed checks
async function retryFailedChecks() {
  if (isRetrying || countFailedChecks() === 0) return;

  isRetrying = true;

  // Create retry progress section if it doesn't exist
  if (!document.getElementById("retryProgressSection")) {
    const progressSection = document.getElementById("progressSection");
    const retryProgressHTML = `
      <div id="retryProgressSection" style="margin-top: 20px; display: none;">
        <div class="section-title">
          <h4><i class="fas fa-redo me-2"></i>Retry Failed Checks</h4>
          <div class="progress" style="height: 10px;">
            <div id="retryProgressBar" class="progress-bar bg-warning" role="progressbar" style="width: 0%"></div>
          </div>
          <div class="d-flex justify-content-between mt-1">
            <small id="retryProgressText">0 / 0 checks</small>
            <small id="retryProgressPercent">0%</small>
          </div>
        </div>
      </div>
    `;
    progressSection.insertAdjacentHTML("beforeend", retryProgressHTML);
  }

  // Show retry progress
  const retryProgressSection = document.getElementById("retryProgressSection");
  retryProgressSection.style.display = "block";

  // Update retry button to show processing
  const retryBtn = document.getElementById("retryBtn");
  retryBtn.innerHTML = `<i class="fas fa-spinner fa-spin me-1"></i> Retrying...`;
  retryBtn.disabled = true;

  const totalChecksToRetry = countFailedChecks();
  updateApiStatus(`Retrying ${totalChecksToRetry} failed checks...`, "warning");

  // Collect all checks to retry
  const checksToRetry = [];
  for (const facilityIndex in failedChecks) {
    for (const checkType in failedChecks[facilityIndex]) {
      checksToRetry.push({
        facilityIndex: parseInt(facilityIndex),
        checkType: checkType,
        facility: failedChecks[facilityIndex][checkType].facility,
        failureType: failedChecks[facilityIndex][checkType].failureType,
      });
    }
  }

  // Process checks in batches
  const batchSize = 5;
  let processedChecks = 0;

  for (let i = 0; i < checksToRetry.length; i += batchSize) {
    const batch = checksToRetry.slice(
      i,
      Math.min(i + batchSize, checksToRetry.length)
    );

    const batchPromises = batch.map((check) =>
      runApiCheck(check.facility, check.facilityIndex, check.checkType, true)
    );

    await Promise.allSettled(batchPromises);

    // Update progress
    processedChecks = Math.min(i + batchSize, checksToRetry.length);
    const percent = Math.round((processedChecks / checksToRetry.length) * 100);
    document.getElementById(
      "retryProgressText"
    ).textContent = `${processedChecks} / ${checksToRetry.length} checks`;
    document.getElementById("retryProgressPercent").textContent = `${percent}%`;
    document.getElementById("retryProgressBar").style.width = `${percent}%`;

    // Small delay between batches
    if (i + batchSize < checksToRetry.length) {
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  // Recalculate category for facilities that had checks retried
  const updatedFacilities = new Set();
  for (const check of checksToRetry) {
    const facility = uploadedData[check.facilityIndex];
    if (facility) {
      facility.category = determineFacilityCategory(facility);
      updatedFacilities.add(check.facilityIndex);

      // Update facility status
      let statusMessage = "";
      switch (facility.category) {
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
          statusMessage = facility.category;
      }
      updateFacilityStatus(check.facilityIndex, "complete", statusMessage);
    }
  }

  // Update UI with results
  updateStatsFromResults(uploadedData);
  updateResultsTable(uploadedData);

  // Update retry button
  isRetrying = false;
  updateRetryButton();

  const remainingFailedChecks = countFailedChecks();
  if (remainingFailedChecks === 0) {
    updateApiStatus("All retries completed successfully!", "success");
    retryProgressSection.style.display = "none";
  } else {
    updateApiStatus(
      `Retry completed. ${remainingFailedChecks} checks still failed.`,
      "warning"
    );
    retryBtn.innerHTML = `<i class="fas fa-redo me-1"></i> Retry Failed (${remainingFailedChecks})`;
    retryBtn.disabled = false;
  }
}

// Enhanced validateSingleFacility function
async function validateSingleFacility(f, index, total) {
  startFacilityTimer(index);

  updateApiStatus(
    `Validating facility ${index + 1}/${total}: ${f.Name}`,
    "info"
  );

  try {
    // Run all API checks
    const checkTypes = [
      "country",
      "admin",
      "road",
      "building",
      "water",
      "population",
    ];

    // Run checks sequentially to ensure dependencies (admin needs country data)
    for (const checkType of checkTypes) {
      await runApiCheck(f, index, checkType, false);
    }

    // Duplicate check (no API call needed)
    const x = parseFloat(f.x);
    const y = parseFloat(f.y);
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

    // Determine category
    f.category = determineFacilityCategory(f);

    // Set status message
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

    f.error = true;
    f.category = "Error";
    return f;
  }
}

// --- Core validation with progress tracking ---
async function validateCoordinates() {
  startTime = Date.now();
  if (totalTimerInterval) clearInterval(totalTimerInterval);
  totalTimerInterval = setInterval(updateTotalElapsedTime, 1000);

  const results = [];
  const batchSize = 5;

  // Show progress section
  document.getElementById("progressSection").style.display = "block";
  document.getElementById("overallProgress").style.display = "block";
  const tracker = document.getElementById("progressTracker");
  tracker.innerHTML = "";

  // Reset failed checks tracking
  failedChecks = {};

  // Create progress cards for all facilities
  uploadedData.forEach((f, i) => {
    tracker.appendChild(createProgressCard(f, i));
  });

  // Hide retry progress section if visible
  const retryProgressSection = document.getElementById("retryProgressSection");
  if (retryProgressSection) {
    retryProgressSection.style.display = "none";
  }

  // Update retry button
  updateRetryButton();

  // Process facilities in batches
  for (let i = 0; i < uploadedData.length; i += batchSize) {
    const batch = uploadedData.slice(i, i + batchSize);

    try {
      const batchPromises = batch.map((f, idx) =>
        validateSingleFacility(f, i + idx, uploadedData.length)
      );

      const batchResults = await Promise.allSettled(batchPromises);

      batchResults.forEach((result, idx) => {
        if (result.status === "fulfilled") {
          results.push(result.value);
        } else {
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
      batch.forEach((f, idx) => {
        f.error = true;
        f.category = "Error";
        results.push(f);
      });
      updateOverallProgress(results.length, uploadedData.length);
    }
  }

  const failedCheckCount = countFailedChecks();
  updateApiStatus(
    `Validation complete: ${results.length} facilities processed. ${failedCheckCount} checks failed.`,
    failedCheckCount > 0 ? "warning" : "success"
  );

  // Update UI with results
  updateMap(results.filter((f) => !f.error));
  updateResultsTable(results);
  updateStatsFromResults(results);

  // Update retry button
  updateRetryButton();

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
  map.eachLayer((l) => {
    if (l instanceof L.Marker) map.removeLayer(l);
  });
  markers = {};

  results.forEach((f, i) => {
    if (f.error) return; // Skip error facilities on map

    // Determine color based on category
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
        color = "#808080"; // Gray for unknown
    }

    const m = L.marker([parseFloat(f.y), parseFloat(f.x)], {
      icon: L.divIcon({
        className: "custom-marker",
        html: `<div style="background-color:${color};width:16px;height:16px;border-radius:50%;border:3px solid white;box-shadow: 0 2px 5px rgba(0,0,0,0.3);"></div>`,
        iconSize: [22, 22],
        iconAnchor: [11, 11],
      }),
    }).addTo(map);

    m.bindPopup(`
          <div style="min-width: 250px;">
            <h6 style="margin: 0 0 10px 0; color: ${color}"><strong>${
      f.Name
    }</strong></h6>
            <p style="margin: 0 0 5px 0;"><strong>Coordinates:</strong> ${
              f.x
            }, ${f.y}</p>
            <p style="margin: 0 0 5px 0;"><strong>Status:</strong> ${
              f.category
            }</p>
            <p style="margin: 0 0 5px 0;"><strong>Country Match:</strong> ${
              f.countryBoundary?.valid ? "✓" : "✗"
            } (${f.countryBoundary?.countryName || "Unknown"})</p>
            <p style="margin: 0 0 5px 0;"><strong>On Water:</strong> ${
              f.waterCheck?.on_water ? "Yes" : "No"
            }</p>
            <p style="margin: 0 0 5px 0;"><strong>Duplicate:</strong> ${
              f.duplicateCheck?.valid ? "No" : "Yes"
            }</p>
            <p style="margin: 0 0 5px 0;"><strong>Road Distance:</strong> ${
              f.roadDistance?.distance !== null
                ? f.roadDistance?.distance.toFixed(2)
                : "N/A"
            }</p>
            <p style="margin: 0 0 5px 0;"><strong>Building Distance:</strong> ${
              f.buildingDistance?.distance !== null
                ? f.buildingDistance?.distance.toFixed(2) < 1
                  ? "At location"
                  : f.buildingDistance?.distance.toFixed(2)
                : "N/A"
            }</p>
            <p style="margin: 0 0 5px 0;">
              <strong>Admin Match:</strong> ${
                f.adminAreaMatch?.valid ? "✓" : "✗"
              }<br>
              <small>Uploaded: ${f.Admin1 || "N/A"}</small><br>
              <small>OSM: ${f.adminAreaMatch?.osmAdminName || "N/A"}</small>
            </p>
          </div>
        `);

    markers[i] = m;
  });

  // Fit map to show all markers if there are any
  const validMarkers = Object.values(markers);
  if (validMarkers.length > 0) {
    const markerGroup = new L.featureGroup(validMarkers);
    map.fitBounds(markerGroup.getBounds().pad(0.1));
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
        : `<div class="admin-comparison">
             <div><small><strong>Uploaded:</strong> ${
               f.Admin1 || "N/A"
             }</small></div>
             <div><small><strong>OSM:</strong> ${
               f.adminAreaMatch?.osmAdminName || "N/A"
             }</small></div>
             <div><span class="badge ${
               f.adminAreaMatch?.valid ? "bg-success" : "bg-warning"
             }">${f.adminAreaMatch?.valid ? "Pass" : "Fail"}</span></div>
           </div>`,
      8: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            f.duplicateCheck?.valid ? "bg-success" : "bg-warning"
          }">${f.duplicateCheck?.valid ? "Pass" : "Fail"}</span>`,
      9: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            f.roadDistance?.valid ? "bg-success" : "bg-warning"
          }">${
            f.roadDistance?.distance !== null
              ? f.roadDistance?.distance.toFixed(2)
              : "N/A"
          }</span>`,
      10: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            !f.waterCheck?.on_water ? "bg-success" : "bg-danger"
          }">${!f.waterCheck?.on_water ? "Pass" : "Fail"}</span>`,
      11: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            f.buildingDistance?.valid ? "bg-success" : "bg-warning"
          }">${
            f.buildingDistance?.distance !== undefined &&
            f.buildingDistance?.distance !== null
              ? f.buildingDistance?.distance.toFixed(2)
              : "N/A"
          }</span>`,
      12: f.error
        ? '<span class="badge bg-secondary">Error</span>'
        : `<span class="badge ${
            f.populationDensity?.valid ? "bg-success" : "bg-warning"
          }">${
            f.populationDensity?.population
              ? f.populationDensity.population.toLocaleString()
              : "N/A"
          }</span>`,
      13: f.error
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

// Download original dataset function
function downloadOriginalDataset() {
  if (!uploadedData || uploadedData.length === 0) {
    alert("No dataset uploaded yet. Please upload a CSV file first.");
    return;
  }

  // Get the original column names from the first row
  const firstRow = uploadedData[0];
  const columns = Object.keys(firstRow);

  // Convert data to CSV format
  const csvContent = [
    columns.join(","), // Header row
    ...uploadedData.map((row) =>
      columns
        .map((col) => {
          const value = row[col];
          // Handle values that might contain commas or quotes
          if (value === null || value === undefined) return "";
          const stringValue = String(value);
          if (
            stringValue.includes(",") ||
            stringValue.includes('"') ||
            stringValue.includes("\n")
          ) {
            return '"' + stringValue.replace(/"/g, '""') + '"';
          }
          return stringValue;
        })
        .join(",")
    ),
  ].join("\n");

  // Create download link
  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const link = document.createElement("a");
  const url = URL.createObjectURL(blob);

  // Get filename from original upload or use default
  const fileInput = document.getElementById("fileUpload");
  let filename = "uploaded_dataset.csv";
  if (fileInput.files.length > 0) {
    filename = fileInput.files[0].name;
  }

  link.setAttribute("href", url);
  link.setAttribute("download", filename);
  link.style.visibility = "hidden";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
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
              <h6><i class="fas fa-redo me-2"></i>Retry System</h6>
              <p>All failed checks (both validation failures and API errors) can be retried. This is useful when:</p>
              <ul>
                <li><strong>Validation failures:</strong> Wrong country, Admin mismatch, on water, no road/building nearby, low population</li>
                <li><strong>API errors:</strong> Network issues, timeouts, server errors</li>
                <li><strong>Retry logic:</strong> Click "Retry Failed (X)" to retry all failed checks. The button shows count of failed checks.</li>
                <li><strong>Visual feedback:</strong> Failed checks show ✗ in the progress tracker with special styling for API errors</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-globe-americas me-2"></i>Country Boundary Check</h6>
              <p>Uses Nominatim reverse geocoding to verify coordinates are within the selected country.</p>
              <ul>
                <li><strong>Source:</strong> OpenStreetMap Nominatim API</li>
                <li><strong>Validation failure:</strong> Wrong country</li>
                <li><strong>Retry benefit:</strong> Coordinates might have been mis-entered or API might return different result</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-water me-2"></i>Water Body Check</h6>
              <p>Checks if coordinates fall on water bodies (rivers, lakes, oceans).</p>
              <ul>
                <li><strong>Source:</strong> Overpass API water features query</li>
                <li><strong>Validation failure:</strong> On water</li>
                <li><strong>Retry benefit:</strong> Water data might be incomplete or coordinates slightly off</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-map-pin me-2"></i>Administrative Area Match</h6>
              <p>Compares Admin1 level from uploaded data with OSM administrative boundaries.</p>
              <ul>
                <li><strong>Source:</strong> OpenStreetMap Nominatim API</li>
                <li><strong>Validation failure:</strong> Admin1 mismatch</li>
                <li><strong>Retry benefit:</strong> OSM data might have been updated or names might match differently</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-road me-2"></i>Road Distance</h6>
              <p>Calculates distance to nearest road using Overture Maps transportation data with Overpass API fallback.</p>
              <ul>
                <li><strong>Primary Source:</strong> Overture Maps (DuckDB query)</li>
                <li><strong>Validation failure:</strong> No road within 500m</li>
                <li><strong>Retry benefit:</strong> Road data might be incomplete or API might return different result</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-building me-2"></i>Building Distance</h6>
              <p>Finds distance to nearest building using Overture Maps building data with Overpass API fallback.</p>
              <ul>
                <li><strong>Primary Source:</strong> Overture Maps (DuckDB query)</li>
                <li><strong>Validation failure:</strong> No building within 200m</li>
                <li><strong>Retry benefit:</strong> Building data might be incomplete or API might return different result</li>
              </ul>
            </div>
            
            <div class="methodology-section">
              <h6><i class="fas fa-users me-2"></i>Population Density</h6>
              <p>Estimates population within ~1km radius using WorldPop dataset.</p>
              <ul>
                <li><strong>Source:</strong> WorldPop Global High Resolution Population (2020)</li>
                <li><strong>Validation failure:</strong> Population ≤ 100 within ~1km</li>
                <li><strong>Retry benefit:</strong> Population data might have been updated</li>
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
                <strong>Retry Strategy:</strong> Use the "Retry Failed" button to retry all failed checks. This can help when:
                <ol>
                  <li>API data has been updated since last check</li>
                  <li>Network issues caused API failures</li>
                  <li>You want to verify if coordinates might now pass validation</li>
                  <li>You've corrected some data and want to re-validate</li>
                </ol>
              </p>
            </div>
            
            <div class="alert alert-info mt-3">
              <i class="fas fa-lightbulb me-2"></i>
              <strong>Note:</strong> All API calls are cached for performance. The retry button allows you to bypass cache and get fresh results for failed checks.
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

.status-retrying {
  color: #ffc107;
  font-weight: 600;
}

.facility-progress.retrying {
  border-left-color: #ffc107;
  background-color: rgba(255, 193, 7, 0.05);
}

.api-check.check-failed {
  background-color: rgba(220, 53, 69, 0.1);
}

.api-check.check-failed:hover {
  background-color: rgba(220, 53, 69, 0.2);
  cursor: pointer;
}

.api-check.check-failed.api-error {
  background-color: rgba(220, 53, 69, 0.2);
  border: 1px solid #dc3545;
}

.api-check.check-failed.api-error:hover {
  background-color: rgba(220, 53, 69, 0.3);
  cursor: pointer;
}

.retry-count-badge {
  background-color: #dc3545;
  color: white;
  border-radius: 10px;
  padding: 2px 8px;
  font-size: 0.75em;
  margin-left: 5px;
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
