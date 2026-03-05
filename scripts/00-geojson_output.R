# =============================================================
# STEP 7 — Lovable artifacts (latest.json + available_runs.json +
#         points_lite_now_* + points_lite_peak_{12,24,48,72}_*)
#
# Lovable fetches from:
#   https://raw.githubusercontent.com/jessicajcss/WRF_CPTEC_rainfall/main/outputs/...
#
# Required (fast path):
#   outputs/points_lite_now_latest.geojson
#   outputs/points_lite_peak_12_latest.geojson
#   outputs/points_lite_peak_24_latest.geojson
#   outputs/points_lite_peak_48_latest.geojson
#   outputs/points_lite_peak_72_latest.geojson
#   outputs/available_runs.json
#   outputs/latest.json   (must contain geojson_url + points_now_url)
# =============================================================

# NOTE: %||% is already defined earlier in the script; keep this only if you removed it above.
`%||%` <- function(a, b) if (!is.null(a)) a else b

compute_heat_weight <- function(rain_mm) {
  # Must match Lovable: Math.min(Math.sqrt(rainMm)/Math.sqrt(50), 1)
  pmin(sqrt(pmax(rain_mm, 0)) / sqrt(50), 1)
}

build_points_lite_from_nested <- function(nested_geojson_path, mode = c("now", "peak"), windowHours = 24L) {
  mode <- match.arg(mode)

  if (!requireNamespace("jsonlite", quietly = TRUE))
    stop("Package 'jsonlite' required.")

  obj <- jsonlite::fromJSON(nested_geojson_path, simplifyVector = FALSE)
  feats <- obj$features %||% list()

  nowUtc <- as.POSIXct(Sys.time(), tz = "UTC")
  windowEnd <- nowUtc + as.numeric(windowHours) * 3600

  out_feats <- lapply(feats, function(f) {
    props <- f$properties %||% list()

    valid <- props$valid_utc
    rain  <- props$rain_mm

    rainNow <- 0
    nowTsBrt <- ""
    peakMm <- 0
    peakTsBrt <- ""

    if (!is.null(valid) && !is.null(rain) && length(valid) > 0 && length(rain) > 0) {
      ts <- as.POSIXct(unlist(valid), tz = "UTC")
      rr <- suppressWarnings(as.numeric(unlist(rain)))

      ok <- is.finite(as.numeric(ts)) & is.finite(rr)
      ts <- ts[ok]
      rr <- rr[ok]

      if (length(ts) > 0) {
        # NOW = closest timestamp to now
        diffs <- abs(as.numeric(difftime(ts, nowUtc, units = "secs")))
        i_now <- which.min(diffs)
        rainNow <- rr[i_now]
        nowTsBrt <- paste0(format(ts[i_now], tz = "America/Sao_Paulo", "%d/%m %H:%M"), " BRT")

        # PEAK within windowHours ahead
        in_win <- ts <= windowEnd
        if (any(in_win)) {
          rr_win <- rr[in_win]
          ts_win <- ts[in_win]
          i_peak <- which.max(rr_win)
          peakMm <- rr_win[i_peak]
          peakTsBrt <- paste0(format(ts_win[i_peak], tz = "America/Sao_Paulo", "%d/%m %H:%M"), " BRT")
        }
      }
    } else {
      # fallback if arrays missing
      rainNow <- props$rain_mm %||% 0
      peakMm  <- props$max_rain_mm %||% rainNow
    }

    list(
      type = "Feature",
      geometry = f$geometry,
      properties = list(
        cep = props$cep %||% props$CEP %||% "",
        municipio = props$municipio %||% props$municipality %||% props$city %||% "",

        rain_now_mm = round(rainNow, 1),
        now_ts_brt = nowTsBrt,

        peak_window_mm = round(peakMm, 1),
        peak_window_ts_brt = peakTsBrt,

        heat_now_weight = compute_heat_weight(rainNow),
        heat_peak_weight = compute_heat_weight(peakMm)
      )
    )
  })

  list(type = "FeatureCollection", features = out_feats)
}

write_points_lite_files <- function(nested_geojson_path, run_dt, out_dir = "outputs", windows = c(12L, 24L, 48L, 72L)) {
  run_id <- format(run_dt, "%Y%m%d%H")

  # NOW (windowHours not used for now-mode, but keep signature consistent)
  now_obj <- build_points_lite_from_nested(nested_geojson_path, mode = "now", windowHours = 24L)
  now_obj$`_meta` <- list(
    run_id = run_id,
    kind = "now",
    generated_utc = format(lubridate::now(tzone = "UTC"), "%Y-%m-%dT%H:%M:%SZ")
  )

  now_run_path <- file.path(out_dir, sprintf("points_lite_now_%s.geojson", run_id))
  now_latest_path <- file.path(out_dir, "points_lite_now_latest.geojson")
  jsonlite::write_json(now_obj, now_run_path, auto_unbox = TRUE, pretty = FALSE)
  file.copy(now_run_path, now_latest_path, overwrite = TRUE)

  # PEAK windows (12/24/48/72)
  for (w in windows) {
    peak_obj <- build_points_lite_from_nested(nested_geojson_path, mode = "peak", windowHours = as.integer(w))
    peak_obj$`_meta` <- list(
      run_id = run_id,
      kind = sprintf("peak_%d", as.integer(w)),
      generated_utc = format(lubridate::now(tzone = "UTC"), "%Y-%m-%dT%H:%M:%SZ")
    )

    peak_run_path <- file.path(out_dir, sprintf("points_lite_peak_%d_%s.geojson", as.integer(w), run_id))
    peak_latest_path <- file.path(out_dir, sprintf("points_lite_peak_%d_latest.geojson", as.integer(w)))

    jsonlite::write_json(peak_obj, peak_run_path, auto_unbox = TRUE, pretty = FALSE)
    file.copy(peak_run_path, peak_latest_path, overwrite = TRUE)
  }

  invisible(TRUE)
}

write_available_runs <- function(run_dt, out_path = "outputs/available_runs.json") {
  run_id <- format(run_dt, "%Y%m%d%H")
  utc_iso <- format(as.POSIXct(run_dt, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ")
  label_brt <- format(as.POSIXct(run_dt, tz = "UTC"), tz = "America/Sao_Paulo", "%d/%m/%Y %H:%M")

  available <- list(
    runs = list(list(
      run_id = run_id,
      run_time_utc = utc_iso,
      label_brt = paste0(label_brt, " BRT")
    )),
    default_run_id = run_id
  )

  jsonlite::write_json(available, out_path, auto_unbox = TRUE, pretty = TRUE)
  invisible(TRUE)
}

write_latest_json <- function(run_dt, out_path = "outputs/latest.json") {
  run_id       <- format(run_dt, "%Y%m%d%H")
  generated_at <- format(lubridate::now(tzone = "UTC"), "%Y-%m-%dT%H:%M:%SZ")

  # IMPORTANT: keys must match Lovable src
  latest <- list(
    run_id = run_id,
    cycle  = run_id,
    generated_at = generated_at,

    # Lovable fallback reads this key:
    geojson_url = sprintf("outputs/wrf_cep_forecast_%s_nested.geojson", run_id),

    # Lovable uses this for precomputed now artifacts:
    points_now_url = sprintf("outputs/points_lite_now_%s.geojson", run_id)
  )

  jsonlite::write_json(latest, out_path, auto_unbox = TRUE, pretty = TRUE)
  invisible(TRUE)
}

if (GHA_MODE) {
  dir.create("outputs", showWarnings = FALSE, recursive = TRUE)

  run_id <- format(run_dt, "%Y%m%d%H")
  nested_path <- file.path("outputs", sprintf("wrf_cep_forecast_%s_nested.geojson", run_id))

  if (!file.exists(nested_path)) {
    stop("Nested GeoJSON not found (expected): ", nested_path)
  }

  # Write all Lovable artifacts
  write_points_lite_files(nested_path, run_dt, out_dir = "outputs", windows = c(12L, 24L, 48L, 72L))
  write_available_runs(run_dt, out_path = "outputs/available_runs.json")
  write_latest_json(run_dt, out_path = "outputs/latest.json")

  # Size safety: keep committed artifacts <100MB
  max_bytes <- 95 * 1024 * 1024
  must_check <- c(
    "outputs/latest.json",
    "outputs/available_runs.json",
    "outputs/points_lite_now_latest.geojson",
    "outputs/points_lite_peak_12_latest.geojson",
    "outputs/points_lite_peak_24_latest.geojson",
    "outputs/points_lite_peak_48_latest.geojson",
    "outputs/points_lite_peak_72_latest.geojson"
  )
  for (f in must_check) {
    if (!file.exists(f)) stop("Missing required artifact: ", f)
    sz <- file.info(f)$size
    message(sprintf("Size check: %s = %.2f MB", f, sz / 1024^2))
    if (is.na(sz) || sz > max_bytes) stop("Artifact too large (>95MB): ", f)
  }

  message("Lovable artifacts written for run_id=", run_id)
}
