###############################################################
# WRF / GRIB2 – hourly rainfall forecast + SC CEP exports
# + Grid export (rain/temp/wind) + Leaflet HTML maps
#
# Step 0 : Configuration
# Step 1 : Download
# Step 2 : Crop & delete (UPDATED: write *_vars.tif with precip+temp+wind)
# Step 3 : Discover files & parse metadata (prefer *_vars.tif)
# Step 4 : Extract precip layer + compute TRUE hourly rain
# Step 5 : Plot by MUNICIPALITY (polygon mean) or by CEP (point)
# Step 6 : CEP all-hours export (nested geojson + optional fgb/shp)
# Step 7 : Lovable artifacts
# Step 8 : Grid export (nested json + optional shp) + 4 HTML maps
#
# Requires: terra, sf, dplyr, ggplot2, lubridate, stringr,
#           rvest, httr, future, furrr, geocodebr, progressr,
#           jsonlite, tibble, viridis, cli, ragg, glue
###############################################################

suppressPackageStartupMessages({
  library(terra)
  library(sf)
  library(dplyr)
  library(ggplot2)
  library(lubridate)
  library(stringr)
  library(rvest)
  library(httr)
  library(future)
  library(furrr)
  library(geocodebr)
})

# =============================================================
# GHA MODE — auto-detected; overrides come from env vars set
# by the GitHub Actions workflow.  Nothing to edit here.
# =============================================================
GHA_MODE <- identical(Sys.getenv("GHA_MODE"), "true")

if (GHA_MODE) {
  # Ensure terra/sf use their own bundled GDAL/PROJ on the runner
  gd <- system.file("gdal", package = "terra")
  pl <- system.file("proj", package = "terra")
  if (nzchar(gd)) Sys.setenv(GDAL_DATA = gd)
  if (nzchar(pl))  Sys.setenv(PROJ_LIB  = pl)
  message("Running in GHA mode.")
}

# =============================================================
# TIMEZONE HELPERS
# =============================================================
BRT_TZ <- "America/Sao_Paulo"

utc_to_brt <- function(x) lubridate::with_tz(x, tzone = BRT_TZ)
fmt_brt_label <- function(x) paste0(format(utc_to_brt(x), "%d/%m %H:%M"), " BRT")
fmt_utc_iso <- function(x) format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ")
fmt_brt_iso <- function(x) format(as.POSIXct(x, tz = "UTC"), tz = BRT_TZ, "%Y-%m-%dT%H:%M:%S%z")

# =============================================================
# STEP 0 — USER CONFIGURATION  (edit this block only)
# =============================================================

## ---- 0a) Desired output mode ----
# "municipality"  -> polygon mean over a Brazilian municipality
# "cep"           -> nearest grid cell to a Brazilian CEP (postcode)
# "all_sc_ceps"   -> always run Step 6/7 (all CEPs) and skip Step 5 target extraction
OUTPUT_MODE <- if (GHA_MODE) "all_sc_ceps" else "municipality"

## ---- 0b) Target location ----
MUNICIPIO_NAME <- "Joinville"   # used when OUTPUT_MODE == "municipality"
CEP            <- "89201-100"   # used when OUTPUT_MODE == "cep"

## ---- 0c) Shapefile (municipalities) ----
SHP_PATH <- "./data/shp/SC_Municipios_2024/SC_Municipios_2024.shp"

## ---- 0d) WRF run ----
RUN_START <- NULL
if (GHA_MODE && nzchar(Sys.getenv("WRF_RUN_START")))
  RUN_START <- Sys.getenv("WRF_RUN_START")

## ---- 0e) Forecast window ----
FORECAST_START_HR <- 0
FORECAST_END_HR   <- 72
if (GHA_MODE && nzchar(Sys.getenv("WRF_FORECAST_END_HR")))
  FORECAST_END_HR <- as.integer(Sys.getenv("WRF_FORECAST_END_HR"))

## ---- 0f) Paths & workers ----
OUTPUT_DIR <- "data/WRF_downloads"
N_WORKERS  <- if (GHA_MODE) 2L else min(8L, max(1L, parallel::detectCores(logical = FALSE)))

## ---- 0g) Pipeline switches ----
CEP_LIST_PATH <- "outputs/cep_list.rds"

## ---- 0h) Export formats ----
EXPORT_FORMATS <- if (GHA_MODE) c("geojson_nested") else c("geojson_nested", "fgb", "shp")

## ---- 0i) Step 8 grid outputs ----
GRID_EXPORT_JSON <- TRUE
GRID_EXPORT_SHP  <- !GHA_MODE       # keep repo small in GHA
GRID_EXPORT_HTML <- TRUE
MAX_GRID_JSON_MB <- 95

SKIP_DOWNLOAD   <- FALSE
SKIP_CROP       <- FALSE
CROP_BUFFER_DEG <- 0.5

# =============================================================
# STEP 1 — DOWNLOADER (original working code preserved)
# =============================================================

parse_yyyymmddhh_utc <- function(x) lubridate::ymd_h(x, tz = "UTC")

get_remote_size <- function(url, timeout_sec = 30) {
  tryCatch({
    h <- httr::HEAD(url, httr::timeout(timeout_sec))
    if (httr::status_code(h) >= 200 && httr::status_code(h) < 400) {
      cl <- httr::headers(h)[["content-length"]]
      if (!is.null(cl) && nzchar(cl)) return(as.numeric(cl))
    }
    NA_real_
  }, error = function(e) NA_real_)
}

write_manifest_csv <- function(df, file_csv, append = TRUE) {
  dir.create(dirname(file_csv), showWarnings = FALSE, recursive = TRUE)
  if (append && file.exists(file_csv)) {
    utils::write.table(df, file_csv, sep = ",", row.names = FALSE,
                       col.names = FALSE, append = TRUE, quote = TRUE)
  } else {
    utils::write.csv(df, file_csv, row.names = FALSE)
  }
}

`%||%` <- function(a, b) if (!is.null(a)) a else b

# Normalise any CEP format to plain 8-digit string (used everywhere)
# Accepts: "89201-100", "89201100", " 89201100 ", 89201100L
normalise_cep <- function(x) {
  formatC(as.integer(gsub("\\D", "", as.character(x))), width = 8L, flag = "0")
}

download_wrf_parallel <- function(
    run_start,
    forecast_start   = 0,
    forecast_end     = 72,
    output_dir       = "data/WRF_downloads",
    n_workers        = 8,
    verbose          = TRUE,
    prefix_regex     = "^WRF_cpt_07KM_",
    include_ext      = NULL,
    exclude_ext      = NULL,
    list_timeout_sec = 60,
    head_timeout_sec = 30,
    method           = "curl",
    by_hour          = TRUE,
    hour_folder_fmt  = "h%03d",
    validate_size    = TRUE,
    retries          = 2,
    retry_wait_sec   = 2,
    manifest         = TRUE,
    manifest_dir     = file.path(output_dir, "_manifests"),
    manifest_name    = NULL,
    manifest_append  = TRUE,
    manifest_format  = c("csv")
) {
  run_dt <- if (is.character(run_start)) lubridate::ymd_hm(run_start, tz = "UTC") else run_start
  if (is.na(run_dt)) stop("Invalid run_start.")

  year  <- lubridate::year(run_dt);   month <- lubridate::month(run_dt)
  day   <- lubridate::day(run_dt);    hour  <- lubridate::hour(run_dt)

  if (!hour %in% c(0, 12))
    warning(sprintf("Run hour is %02dZ. Many ops cycles are 00Z/12Z, but continuing.", hour))

  base_url <- sprintf(
    "https://dataserver.cptec.inpe.br/dataserver_modelos/wrf/ams_07km/brutos/%04d/%02d/%02d/%02d/",
    year, month, day, hour)

  # Local root for this run: OUTPUT_DIR/YYYYMMDD/
  dated_dir <- file.path(output_dir, format(run_dt, "%Y%m%d"))
  dir.create(dated_dir, showWarnings = FALSE, recursive = TRUE)

  if (verbose) {
    cat("================================================================\n")
    cat(sprintf("Run init  : %s UTC (%02dZ)\n", format(run_dt, "%Y-%m-%d %H:%M"), hour))
    cat(sprintf("Run init  : %s\n", fmt_brt_label(run_dt)))
    cat(sprintf("Fcst hrs  : %d to %d\n", forecast_start, forecast_end))
    cat(sprintf("Base URL  : %s\n", base_url))
    cat(sprintf("Local dir : %s\n", dated_dir))
    if (!is.null(exclude_ext)) cat(sprintf("Exclude   : %s\n", paste(exclude_ext, collapse=", ")))
    cat("================================================================\n\n")
  }

  if (verbose) cat("Fetching file list from server...\n")
  resp <- httr::GET(base_url, httr::timeout(list_timeout_sec))
  if (httr::status_code(resp) != 200)
    stop(sprintf("HTTP %d accessing %s", httr::status_code(resp), base_url))

  links     <- rvest::html_attr(rvest::html_nodes(rvest::read_html(resp), "a"), "href")
  wrf_files <- links[grepl(prefix_regex, links)]
  if (!length(wrf_files)) stop("No WRF files found at URL.")

  p1 <- "^WRF_cpt_07KM_(\\d{10})_(\\d{10})\\.(.+)$"
  p2 <- "^WRF_cpt_07KM_(\\d{10})\\.(.+)$"
  f1 <- wrf_files[grepl(p1, wrf_files)]
  f2 <- wrf_files[grepl(p2, wrf_files) & !grepl(p1, wrf_files)]

  make_df <- function(files, pat, rg, fg, eg) {
    if (!length(files)) return(NULL)
    data.frame(filename=files, run_time=sub(pat,rg,files),
               forecast_time=sub(pat,fg,files), extension=sub(pat,eg,files),
               stringsAsFactors=FALSE)
  }
  file_df <- rbind(make_df(f1,p1,"\\1","\\2","\\3"),
                   make_df(f2,p2,"\\1","\\1","\\2"))
  if (!nrow(file_df)) stop("Zero files after pattern parsing.")

  file_df$run_dt        <- parse_yyyymmddhh_utc(file_df$run_time)
  file_df$forecast_dt   <- parse_yyyymmddhh_utc(file_df$forecast_time)
  file_df$forecast_hour <- as.numeric(
    difftime(file_df$forecast_dt, file_df$run_dt, units="hours"))

  if (!is.null(include_ext)) file_df <- file_df[file_df$extension %in% include_ext, ]
  if (!is.null(exclude_ext)) file_df <- file_df[!file_df$extension %in% exclude_ext, ]
  file_df <- file_df[file_df$forecast_hour >= forecast_start &
                       file_df$forecast_hour <= forecast_end, ]
  if (!nrow(file_df)) { cat("No files match the specified window.\n"); return(invisible(NULL)) }

  if (verbose) {
    ext_tab <- table(file_df$extension)
    cat(sprintf("Files in window: %d\n", nrow(file_df)))
    for (ext in names(ext_tab)) cat(sprintf("  %-8s: %d\n", ext, ext_tab[[ext]]))
    cat("\n")
  }

  file_df$hour_folder <- if (by_hour) sprintf(hour_folder_fmt, as.integer(file_df$forecast_hour)) else ""
  file_df$local_dir   <- if (by_hour) file.path(dated_dir, file_df$hour_folder) else dated_dir
  file_df$local_path  <- file.path(file_df$local_dir, file_df$filename)
  file_df$url         <- paste0(base_url, file_df$filename)

  for (d in unique(file_df$local_dir)) dir.create(d, showWarnings=FALSE, recursive=TRUE)

  # A file is "done" if GRIB2 OR its cropped *_vars.tif already exists
  tif_paths <- sub("\\.grib2$", "_vars.tif", file_df$local_path, ignore.case=TRUE)
  file_df$exists <- file.exists(file_df$local_path) | file.exists(tif_paths)
  to_get <- file_df[!file_df$exists, ]

  if (verbose)
    cat(sprintf("Already on disk: %d  |  To download: %d\n\n",
                sum(file_df$exists), nrow(to_get)))

  if (!nrow(to_get)) {
    cat("All requested files already exist on disk. Skipping download.\n")
    return(invisible(list(successful=0L, failed=0L,
                          skipped=nrow(file_df), run_dir=dated_dir)))
  }

  if (!requireNamespace("progressr", quietly=TRUE))
    stop("Package 'progressr' required. Install with: install.packages('progressr')")
  library(progressr)
  if (requireNamespace("cli", quietly=TRUE)) progressr::handlers("cli") else
    progressr::handlers("txtprogressbar")

  future::plan(future::multisession, workers=n_workers)
  t0 <- Sys.time()

  dl_one <- function(p, url, destfile, filename, fhour, ext, local_dir, valid_dt) {
    dir.create(local_dir, showWarnings=FALSE, recursive=TRUE)
    remote_bytes <- if (validate_size) get_remote_size(url, head_timeout_sec) else NA_real_
    attempt  <- 0L
    max_att  <- as.integer(1L + retries)
    while (attempt < max_att) {
      attempt <- attempt + 1L
      if (file.exists(destfile)) try(unlink(destfile), silent=TRUE)
      ok <- tryCatch({
        utils::download.file(url, destfile, mode="wb", quiet=TRUE, method=method); TRUE
      }, error=function(e) FALSE)
      if (!ok || !file.exists(destfile)) { if (attempt < max_att) Sys.sleep(retry_wait_sec); next }
      local_bytes <- file.info(destfile)$size
      if (validate_size && !is.na(remote_bytes) && is.finite(remote_bytes)) {
        if (!isTRUE(all.equal(as.numeric(local_bytes), as.numeric(remote_bytes)))) {
          try(unlink(destfile), silent=TRUE)
          if (attempt < max_att) Sys.sleep(retry_wait_sec); next
        }
      }
      p(sprintf("h%03d %s", as.integer(fhour), filename))
      return(list(success=TRUE, file=filename, hour=fhour, ext=ext, attempts=attempt,
                  local_bytes=local_bytes, local_mb=local_bytes/1024^2,
                  remote_bytes=remote_bytes, valid_dt=valid_dt))
    }
    p(sprintf("FAILED h%03d %s", as.integer(fhour), filename))
    list(success=FALSE, file=filename, hour=fhour, ext=ext, attempts=attempt,
         error="Download or size validation failed", remote_bytes=remote_bytes,
         valid_dt=valid_dt, local_bytes=NA_real_, local_mb=NA_real_)
  }

  progressr::with_progress({
    p <- progressr::progressor(steps=nrow(to_get))
    results <- furrr::future_pmap(
      list(p=replicate(nrow(to_get), p, simplify=FALSE),
           url=to_get$url, destfile=to_get$local_path,
           filename=to_get$filename, fhour=to_get$forecast_hour,
           ext=to_get$extension, local_dir=to_get$local_dir,
           valid_dt=to_get$forecast_dt),
      dl_one, .options=furrr::furrr_options(seed=TRUE))
  })
  future::plan(future::sequential)

  elapsed    <- as.numeric(difftime(Sys.time(), t0, units="secs"))
  successful <- sum(vapply(results, function(x) isTRUE(x$success), logical(1)))
  failed     <- length(results) - successful
  total_mb   <- sum(vapply(results, function(x) if(isTRUE(x$success)) x$local_mb else 0, numeric(1)))

  if (verbose) {
    cat("\n")
    for (i in seq_along(results)) {
      r <- results[[i]]
      if (isTRUE(r$success))
        cat(sprintf("[%3d/%3d] OK  h%03d | %7.2f MB | tries=%d | %s\n",
                    i, length(results), r$hour, r$local_mb, r$attempts, r$file))
      else
        cat(sprintf("[%3d/%3d] FAIL h%03d | tries=%d | %s\n",
                    i, length(results), r$hour, r$attempts, r$file))
    }
    cat(sprintf("\nDownloaded %d files (%.2f MB) in %.1f s | %d failed\n",
                successful, total_mb, elapsed, failed))
  }

  if (manifest) {
    dir.create(manifest_dir, showWarnings=FALSE, recursive=TRUE)
    if (is.null(manifest_name))
      manifest_name <- sprintf("manifest_WRF_%s_h%d-%d",
                               format(run_dt,"%Y%m%d%H"), forecast_start, forecast_end)
    files <- vapply(results, `[[`, character(1), "file")
    res_df <- data.frame(
      run_init_utc   = format(run_dt, "%Y-%m-%d %H:%M:%S"),
      valid_time_utc = vapply(results, function(x) format(x$valid_dt, "%Y-%m-%d %H:%M:%S"), character(1)),
      forecast_hour  = vapply(results, function(x) as.numeric(x$hour), numeric(1)),
      filename       = files,
      extension      = vapply(results, `[[`, character(1), "ext"),
      url            = to_get$url[match(files, to_get$filename)],
      local_path     = to_get$local_path[match(files, to_get$filename)],
      success        = vapply(results, function(x) isTRUE(x$success), logical(1)),
      attempts       = as.integer(vapply(results, function(x) x$attempts, numeric(1))),
      remote_bytes   = vapply(results, function(x) x$remote_bytes %||% NA_real_, numeric(1)),
      local_bytes    = vapply(results, function(x) x$local_bytes  %||% NA_real_, numeric(1)),
      error          = vapply(results, function(x)
        if (!isTRUE(x$success)) x$error %||% NA_character_ else NA_character_, character(1)),
      stringsAsFactors = FALSE
    )
    write_manifest_csv(res_df,
                       file.path(manifest_dir, paste0(manifest_name,".csv")),
                       append=manifest_append)
    if (verbose) cat(sprintf("Manifest: %s/%s.csv\n", manifest_dir, manifest_name))
  }

  invisible(list(successful=successful, failed=failed, skipped=sum(file_df$exists),
                 total_mb=total_mb, elapsed_seconds=elapsed, run_dir=dated_dir))
}

# Auto-detect latest available 00Z or 12Z run
detect_latest_run <- function(max_lookback_days=3, timeout_sec=30) {
  now_utc <- lubridate::now(tzone="UTC")
  candidates <- c()
  for (d in 0:max_lookback_days) {
    day <- now_utc - lubridate::days(d)
    for (hh in c(12, 0)) {
      cand <- lubridate::floor_date(day,"day") + lubridate::hours(hh)
      if (cand <= now_utc) candidates <- c(candidates, cand)
    }
  }
  for (run_dt in candidates) {
    run_dt <- as.POSIXct(run_dt, tz="UTC", origin="1970-01-01")
    url <- sprintf(
      "https://dataserver.cptec.inpe.br/dataserver_modelos/wrf/ams_07km/brutos/%04d/%02d/%02d/%02d/",
      lubridate::year(run_dt), lubridate::month(run_dt),
      lubridate::day(run_dt),  lubridate::hour(run_dt))
    resp <- tryCatch(httr::HEAD(url, httr::timeout(timeout_sec)), error=function(e) NULL)
    if (!is.null(resp) && httr::status_code(resp) %in% 200:399) {
      message(sprintf("Auto-detected latest run: %s UTC (%s)",
                      format(run_dt,"%Y-%m-%d %H:%M"), fmt_brt_label(run_dt)))
      return(run_dt)
    }
  }
  stop("Could not find a reachable WRF run. Set RUN_START manually.")
}

# =============================================================
# STEP 2 — CROP & DELETE (UPDATED: multi-variable stack)
# =============================================================

pick_precip_layer <- function(r, verbose=FALSE) {
  nm <- names(r)
  try_idx <- function(pat) grep(pat, nm, ignore.case=TRUE)

  idx <- try_idx("Total precipitation")
  if (length(idx)) { if(verbose) message("  layer: ", nm[idx[1]]); return(r[[idx[1]]]) }

  ic <- try_idx("Convective precipitation")
  il <- try_idx("Large.scale precipitation|Large scale precipitation")
  if (length(ic) && length(il)) {
    if(verbose) message("  layer: conv + large-scale sum"); return(r[[ic[1]]] + r[[il[1]]])
  }
  if (length(ic)) { if(verbose) message("  layer: Convective only"); return(r[[ic[1]]]) }
  if (length(il)) { if(verbose) message("  layer: Large-scale only"); return(r[[il[1]]]) }

  idx <- try_idx("APCP")
  if (length(idx)) { if(verbose) message("  layer: APCP"); return(r[[idx[1]]]) }

  idx <- try_idx("precip|rain|prate|tp")
  if (length(idx)) { if(verbose) message("  layer: keyword (",nm[idx[1]],")"); return(r[[idx[1]]]) }

  stop("No precipitation layer found.\nLayer names:\n", paste(nm, collapse="\n"))
}

pick_temp2m_layer <- function(r, verbose = FALSE) {
  nm <- names(r)
  try_idx <- function(pat) grep(pat, nm, ignore.case=TRUE)

  idx <- try_idx("2 metre temperature|2m temperature|TMP.*2 m|T2M|t2m")
  if (length(idx)) { if(verbose) message("  layer: ", nm[idx[1]]); return(r[[idx[1]]]) }

  # fallback: any temperature
  idx <- try_idx("temperature|TMP")
  if (length(idx)) { if(verbose) message("  layer: ", nm[idx[1]]); return(r[[idx[1]]]) }

  NULL
}

pick_wind_gust_layer <- function(r, verbose = FALSE) {
  nm <- names(r)
  try_idx <- function(pat) grep(pat, nm, ignore.case=TRUE)

  idx <- try_idx("gust|GUST")
  if (length(idx)) { if(verbose) message("  layer: ", nm[idx[1]]); return(r[[idx[1]]]) }

  # fallback: wind speed 10m
  idx <- try_idx("10 metre.*wind|10m wind|wind speed|WIND10|WSPD10")
  if (length(idx)) { if(verbose) message("  layer: ", nm[idx[1]]); return(r[[idx[1]]]) }

  NULL
}

get_crop_extent <- function(shp_path, buffer_deg=0.5) {
  v   <- terra::vect(shp_path)
  ext <- terra::ext(v)
  terra::ext(ext$xmin - buffer_deg, ext$xmax + buffer_deg,
             ext$ymin - buffer_deg, ext$ymax + buffer_deg)
}

crop_and_save_one <- function(grib2_path, crop_ext_lonlat, verbose=FALSE) {
  # NEW output
  tif_path <- sub("\\.grib2$", "_vars.tif", grib2_path, ignore.case=TRUE)

  if (file.exists(tif_path) && !file.exists(grib2_path)) return(tif_path)
  if (file.exists(tif_path) && file.exists(grib2_path)) {
    file.remove(grib2_path); return(tif_path)
  }

  tryCatch({
    r <- terra::rast(grib2_path)

    p <- pick_precip_layer(r, verbose=verbose)
    t <- pick_temp2m_layer(r, verbose=verbose)
    w <- pick_wind_gust_layer(r, verbose=verbose)

    if (is.null(t)) warning("Temp layer not found in: ", basename(grib2_path))
    if (is.null(w)) warning("Wind layer not found in: ", basename(grib2_path))

    crop_ext_use <- if (!terra::is.lonlat(p)) {
      pts <- terra::vect(
        data.frame(x=c(crop_ext_lonlat$xmin, crop_ext_lonlat$xmax,
                       crop_ext_lonlat$xmin, crop_ext_lonlat$xmax),
                   y=c(crop_ext_lonlat$ymin, crop_ext_lonlat$ymin,
                       crop_ext_lonlat$ymax, crop_ext_lonlat$ymax)),
        geom=c("x","y"), crs="EPSG:4326")
      terra::ext(terra::project(pts, terra::crs(p)))
    } else crop_ext_lonlat

    p_crop <- terra::crop(p, crop_ext_use)

    if (!is.null(t)) {
      t_crop <- terra::crop(t, crop_ext_use)
      if (!isTRUE(all.equal(terra::res(t_crop), terra::res(p_crop))) ||
          !isTRUE(all.equal(terra::ext(t_crop), terra::ext(p_crop))))
        t_crop <- terra::resample(t_crop, p_crop, method = "bilinear")
    } else {
      t_crop <- p_crop; terra::values(t_crop) <- NA_real_
    }

    if (!is.null(w)) {
      w_crop <- terra::crop(w, crop_ext_use)
      if (!isTRUE(all.equal(terra::res(w_crop), terra::res(p_crop))) ||
          !isTRUE(all.equal(terra::ext(w_crop), terra::ext(p_crop))))
        w_crop <- terra::resample(w_crop, p_crop, method = "bilinear")
    } else {
      w_crop <- p_crop; terra::values(w_crop) <- NA_real_
    }

    vars <- c(p_crop, t_crop, w_crop)
    names(vars) <- c("precip_acc_mm", "t2m_k", "wind_gust_ms")

    terra::writeRaster(vars, tif_path, overwrite=TRUE, gdal=c("COMPRESS=LZW"))
    file.remove(grib2_path)
    tif_path
  }, error=function(e) {
    warning(sprintf("Crop failed for %s:\n  %s\n  File kept as-is.",
                    basename(grib2_path), conditionMessage(e)))
    NA_character_
  })
}

crop_run_directory <- function(run_dir, shp_path, buffer_deg=0.5, verbose=TRUE) {
  grib2_files <- list.files(run_dir, pattern="\\.grib2$",
                            full.names=TRUE, recursive=TRUE)
  if (!length(grib2_files)) {
    message("No .grib2 files to crop in: ", run_dir, " (already processed?)")
    return(invisible(NULL))
  }

  crop_ext <- get_crop_extent(shp_path, buffer_deg)
  size_before <- sum(file.info(grib2_files)$size, na.rm=TRUE) / 1024^2

  if (verbose) {
    cat(sprintf("\nCropping %d GRIB2 files → *_vars.tif (precip+temp+wind) + %.1f deg buffer...\n",
                length(grib2_files), buffer_deg))
  }

  results <- vapply(seq_along(grib2_files), function(i) {
    f <- grib2_files[i]
    if (verbose) message(sprintf("  crop [%d/%d] %s", i, length(grib2_files), basename(f)))
    crop_and_save_one(f, crop_ext, verbose=FALSE)
  }, character(1))

  tif_ok     <- results[!is.na(results)]
  size_after <- sum(file.info(tif_ok)$size, na.rm=TRUE) / 1024^2

  if (verbose) {
    cat(sprintf("\nCrop complete: %d / %d files converted to *_vars.tif\n",
                length(tif_ok), length(grib2_files)))
    cat(sprintf("Disk usage: %.1f MB (GRIB2)  ->  %.1f MB (vars.tif)\n\n",
                size_before, size_after))
  }
  invisible(results)
}

# =============================================================
# STEP 3 — FILE DISCOVERY & METADATA
# Prefer *_vars.tif; fallback to *.tif; fallback to *.grib2
# =============================================================

discover_wrf_files <- function(run_dir) {
  files <- list.files(run_dir, pattern="_vars\\.tif$", full.names=TRUE, recursive=TRUE)
  ext_used <- "vars.tif"

  if (!length(files)) {
    files <- list.files(run_dir, pattern="\\.tif$", full.names=TRUE, recursive=TRUE)
    ext_used <- "tif"
  }
  if (!length(files)) {
    files    <- list.files(run_dir, pattern="\\.grib2$", full.names=TRUE, recursive=TRUE)
    ext_used <- "grib2"
  }
  if (!length(files)) stop("No .tif or .grib2 files found under: ", run_dir)
  message(sprintf("Discovered %d .%s files.", length(files), ext_used))

  base <- tools::file_path_sans_ext(basename(files))
  base <- sub("_vars$", "", base)

  p1 <- "^WRF_cpt_07KM_(\\d{10})_(\\d{10})$"
  p2 <- "^WRF_cpt_07KM_(\\d{10})$"
  is_p1 <- grepl(p1, base)
  is_p2 <- grepl(p2, base) & !is_p1

  init_str  <- dplyr::case_when(is_p1~sub(p1,"\\1",base), is_p2~sub(p2,"\\1",base), TRUE~NA_character_)
  valid_str <- dplyr::case_when(is_p1~sub(p1,"\\2",base), is_p2~sub(p2,"\\1",base), TRUE~NA_character_)

  bad <- is.na(init_str) | is.na(valid_str)
  if (any(bad)) {
    warning(sum(bad), " file(s) dropped (unrecognised naming pattern)")
    files <- files[!bad]; init_str <- init_str[!bad]; valid_str <- valid_str[!bad]
  }

  init_utc  <- lubridate::ymd_h(init_str,  tz="UTC")
  valid_utc <- lubridate::ymd_h(valid_str, tz="UTC")

  tibble::tibble(
    file          = files,
    init_utc      = init_utc,
    forecast_hour = as.numeric(difftime(valid_utc, init_utc, units="hours")),
    valid_utc     = valid_utc,
    valid_brt_lbl = vapply(valid_utc, fmt_brt_label, character(1))
  ) |> dplyr::arrange(init_utc, forecast_hour)
}

# =============================================================
# STEP 4 — EXTRACT & CONVERT TO TRUE HOURLY RAIN
# (still rain-only for Step 5)
# =============================================================

extract_precip_acc_layer <- function(r) {
  if (terra::nlyr(r) == 1) return(r)
  nm <- names(r)
  if ("precip_acc_mm" %in% nm) return(r[["precip_acc_mm"]])
  pick_precip_layer(r, verbose = FALSE)
}

extract_one_file <- function(file, target_vect, fun=mean, verbose=FALSE) {
  if (!file.exists(file))          { warning("Not found: ", file);          return(NA_real_) }
  if (file.info(file)$size < 100L) { warning("Too small: ", basename(file)); return(NA_real_) }

  tryCatch({
    r <- terra::rast(file)
    p <- extract_precip_acc_layer(r)

    if (!terra::same.crs(target_vect, p))
      target_vect <- terra::project(target_vect, terra::crs(p))

    v <- terra::extract(p, target_vect, fun=fun, na.rm=TRUE)
    as.numeric(v[1, 2])
  }, error=function(e) {
    warning(sprintf("extract_one_file failed [%s]: %s", basename(file), conditionMessage(e)))
    NA_real_
  })
}

extract_run_accum <- function(meta, target_vect, fun=mean, verbose=TRUE) {
  out <- vector("list", nrow(meta))
  for (i in seq_len(nrow(meta))) {
    if (verbose) message(sprintf("  [%d/%d] h%03d  %s (%s)",
                                 i, nrow(meta), meta$forecast_hour[i], basename(meta$file[i]), meta$valid_brt_lbl[i]))
    out[[i]] <- tibble::tibble(
      init_utc      = meta$init_utc[i],
      forecast_hour = meta$forecast_hour[i],
      valid_utc     = meta$valid_utc[i],
      accum_mm      = extract_one_file(meta$file[i], target_vect, fun, verbose=FALSE)
    )
  }
  dplyr::bind_rows(out)
}

accum_to_hourly <- function(df) {
  df |>
    dplyr::arrange(init_utc, forecast_hour) |>
    dplyr::group_by(init_utc) |>
    dplyr::mutate(
      rain_mm = dplyr::if_else(
        dplyr::row_number() == 1L,
        NA_real_,
        pmax(0, accum_mm - dplyr::lag(accum_mm))
      )
    ) |>
    dplyr::ungroup()
}

extract_rain_municipality <- function(run_dir, shp_path, municipio_name,
                                      name_col=NULL, verbose=TRUE) {
  if (verbose) cat("Discovering files...\n")
  meta <- discover_wrf_files(run_dir)

  if (verbose) cat("Loading municipality polygon...\n")
  v <- terra::vect(shp_path)
  if (is.null(name_col)) {
    cands    <- c("NM_MUN","NM_MUNICIP","NOME","NAME","MUNICIPIO")
    name_col <- intersect(names(v), cands)[1]
    if (is.na(name_col))
      stop("Cannot auto-detect name column. Available: ", paste(names(v), collapse=", "))
  }
  vals <- as.character(v[[name_col]])
  idx  <- which(tolower(vals) == tolower(municipio_name))
  if (!length(idx)) idx <- grep(tolower(municipio_name), tolower(vals))
  if (!length(idx)) stop("Municipality not found: ", municipio_name)
  poly <- v[idx[1]]

  if (verbose) cat(sprintf("Extracting over %s (%d files)...\n", municipio_name, nrow(meta)))
  raw <- extract_run_accum(meta, poly, fun=mean, verbose=verbose)
  accum_to_hourly(raw)
}

extract_rain_cep <- function(run_dir, cep, cep_list_df = NULL, verbose = TRUE) {
  if (verbose) cat("Discovering files...\n")
  meta <- discover_wrf_files(run_dir)

  cep_clean <- normalise_cep(cep)
  if (nchar(cep_clean) != 8 || is.na(suppressWarnings(as.integer(cep_clean))))
    stop("Invalid CEP: ", cep, " -> normalised to '", cep_clean, "'")

  cep_fmt <- paste0(substr(cep_clean,1,5), "-", substr(cep_clean,6,8))

  lon <- NA_real_; lat <- NA_real_
  if (!is.null(cep_list_df)) {
    cep_list_df$cep_norm <- normalise_cep(cep_list_df$cep)
    hit <- cep_list_df[cep_list_df$cep_norm == cep_clean, ]
    if (nrow(hit)) {
      lon <- hit$lon[1]; lat <- hit$lat[1]
      if (verbose)
        cat(sprintf("CEP %s found in cep_list: %s  (%.5f, %.5f)\n",
                    cep_fmt,
                    hit$label[1] %||% hit$municipio[1] %||% cep_fmt,
                    lon, lat))
    }
  }

  if (is.na(lon) || is.na(lat)) {
    if (verbose) cat(sprintf("CEP %s not in cep_list — geocoding via geocodebr...\n", cep_fmt))
    geo <- geocodebr::busca_por_cep(cep_clean)
    if (!nrow(geo)) stop("CEP not found: ", cep_fmt)
    lon_col <- intersect(c("lon","longitude","x"), names(geo))[1]
    lat_col <- intersect(c("lat","latitude","y"),  names(geo))[1]
    if (is.na(lon_col) || is.na(lat_col))
      stop("No lon/lat in geocodebr output. Columns: ", paste(names(geo), collapse=", "))
    lon <- as.numeric(geo[[lon_col]][1])
    lat <- as.numeric(geo[[lat_col]][1])
  }

  pt <- terra::vect(
    data.frame(lon = lon, lat = lat),
    geom = c("lon","lat"), crs = "EPSG:4326")

  if (verbose) cat(sprintf("Extracting at CEP %s (%d files)...\n", cep_fmt, nrow(meta)))
  raw <- extract_run_accum(meta, pt, fun = mean, verbose = verbose)
  accum_to_hourly(raw)
}

# =============================================================
# STEP 5 — PLOTTING
# =============================================================

plot_rain_bar <- function(df, title=NULL, subtitle=NULL) {
  df_plot <- dplyr::filter(df, !is.na(rain_mm))
  if (!nrow(df_plot)) {
    warning("No non-NA rain_mm values — check accum_mm column for issues.")
    cat("\nFirst 10 rows of df:\n"); print(head(df, 10))
    return(invisible(NULL))
  }
  p <- ggplot2::ggplot(df_plot, ggplot2::aes(x=valid_utc, y=rain_mm)) +
    ggplot2::geom_col(fill="#2166ac", width=3600 * 0.9) +
    ggplot2::scale_x_datetime(date_labels="%d/%m\n%H:%M", date_breaks="6 hours") +
    ggplot2::labs(title=title, subtitle=subtitle,
                  x="Forecast valid time (UTC)", y="Rainfall (mm / hour)") +
    ggplot2::theme_minimal(base_size=13) +
    ggplot2::theme(
      plot.title       = ggplot2::element_text(face="bold"),
      plot.subtitle    = ggplot2::element_text(color="grey40"),
      panel.grid.minor = ggplot2::element_blank()
    )
  if (identical(Sys.getenv("GHA_MODE"), "true")) invisible(p) else print(p)
  invisible(p)
}

# =============================================================
# MAIN EXECUTION
# =============================================================

run_dt <- if (is.null(RUN_START)) detect_latest_run() else
  lubridate::ymd_hm(RUN_START, tz="UTC")

run_dir <- file.path(OUTPUT_DIR, format(run_dt, "%Y%m%d"))
cat(sprintf("\nRun init: %s UTC | %s\n", format(run_dt, "%Y-%m-%d %H:%M"), fmt_brt_label(run_dt)))

if (!SKIP_DOWNLOAD) {
  download_wrf_parallel(
    run_start      = run_dt,
    forecast_start = FORECAST_START_HR,
    forecast_end   = FORECAST_END_HR,
    output_dir     = OUTPUT_DIR,
    n_workers      = N_WORKERS,
    exclude_ext    = c("inv"),
    by_hour        = TRUE,
    validate_size  = TRUE,
    manifest       = TRUE
  )
} else {
  message("SKIP_DOWNLOAD = TRUE  —  using files in: ", run_dir)
}

gc()

if (!SKIP_CROP) {
  crop_run_directory(run_dir, SHP_PATH,
                     buffer_deg = CROP_BUFFER_DEG,
                     verbose    = TRUE)
} else {
  message("SKIP_CROP = TRUE  —  expecting *_vars.tif files already in: ", run_dir)
}

# Step 5 (single target) unless in all-sc mode
if (OUTPUT_MODE %in% c("municipality", "cep")) {
  if (OUTPUT_MODE == "municipality") {
    df <- extract_rain_municipality(run_dir, SHP_PATH, MUNICIPIO_NAME, verbose=TRUE)
  } else {
    cep_list_df <- if (file.exists(CEP_LIST_PATH)) readRDS(CEP_LIST_PATH) else NULL
    if (is.null(cep_list_df))
      message("cep_list.rds not found — will geocode via geocodebr (slower).")
    df <- extract_rain_cep(run_dir, CEP, cep_list_df = cep_list_df, verbose = TRUE)
  }

  plot_rain_bar(
    df,
    title    = sprintf("%s — WRF hourly rainfall forecast",
                       if (OUTPUT_MODE=="municipality") MUNICIPIO_NAME else paste0("CEP ", CEP)),
    subtitle = sprintf("Run init: %s UTC  |  %s",
                       format(run_dt, "%Y-%m-%d %H:%M"),
                       if (OUTPUT_MODE=="municipality") "mean over municipality polygon"
                       else "nearest grid cell")
  )

  cat("\nHourly rainfall table (mm):\n")
  print(dplyr::filter(df, !is.na(rain_mm)) |>
          dplyr::select(valid_utc, forecast_hour, accum_mm, rain_mm),
        n = Inf)
} else {
  message("OUTPUT_MODE=", OUTPUT_MODE, " — skipping single-target extraction/plot.")
}

# =============================================================
# STEP 6 — SPATIAL EXPORT  (GeoJSON + Shapefile)
# (YOUR ORIGINAL STEP 6 CODE KEPT, WITH PRECIP LAYER FIX APPLIED)
# =============================================================

classify_rain <- function(x) {
  dplyr::case_when(
    is.na(x)  ~ "no data",
    x == 0    ~ "none",
    x <  2.5  ~ "light",
    x <  7.5  ~ "moderate",
    x < 35.0  ~ "heavy",
    TRUE      ~ "extreme"
  )
}

geocode_ceps <- function(cep_list) {
  if (is.null(names(cep_list))) names(cep_list) <- cep_list

  rows <- lapply(names(cep_list), function(label) {
    cep       <- cep_list[[label]]
    cep_clean <- gsub("\\D", "", cep)
    geo <- tryCatch(geocodebr::busca_por_cep(cep_clean), error = function(e) NULL)
    if (is.null(geo) || !nrow(geo)) {
      warning("CEP not found: ", cep); return(NULL)
    }
    lon_col <- intersect(c("lon","longitude","x"), names(geo))[1]
    lat_col <- intersect(c("lat","latitude","y"),  names(geo))[1]
    if (is.na(lon_col) || is.na(lat_col)) {
      warning("No lon/lat for CEP: ", cep); return(NULL)
    }
    data.frame(
      cep   = cep,
      label = label,
      lon   = as.numeric(geo[[lon_col]][1]),
      lat   = as.numeric(geo[[lat_col]][1]),
      stringsAsFactors = FALSE
    )
  })

  rows <- rows[!vapply(rows, is.null, logical(1))]
  if (!length(rows)) stop("No CEPs could be geocoded.")

  pts <- do.call(rbind, rows)
  sf::st_as_sf(pts, coords = c("lon","lat"), crs = 4674, remove = FALSE)
}

extract_rain_multi_cep <- function(run_dir, cep_sf, verbose = TRUE) {
  meta <- discover_wrf_files(run_dir)
  pts_vect <- terra::vect(cep_sf)

  all_out <- vector("list", nrow(meta))

  for (i in seq_len(nrow(meta))) {
    fh  <- meta$forecast_hour[i]
    vdt <- meta$valid_utc[i]
    if (verbose) message(sprintf("  [%d/%d] h%03d  %s (%s)",
                                 i, nrow(meta), fh, basename(meta$file[i]), meta$valid_brt_lbl[i]))

    f <- meta$file[i]
    if (!file.exists(f) || file.info(f)$size < 100L) {
      warning("Skipping missing/small file: ", basename(f))
      next
    }

    vals <- tryCatch({
      r <- terra::rast(f)
      p <- extract_precip_acc_layer(r)   # <- fix: uses precip_acc_mm when present
      pts2 <- if (!terra::same.crs(pts_vect, p))
        terra::project(pts_vect, terra::crs(p)) else pts_vect
      v <- terra::extract(p, pts2, fun = mean, na.rm = TRUE)
      as.numeric(v[, 2])
    }, error = function(e) {
      warning(sprintf("h%03d extract failed: %s", fh, conditionMessage(e)))
      rep(NA_real_, nrow(cep_sf))
    })

    all_out[[i]] <- data.frame(
      init_utc      = meta$init_utc[i],
      forecast_hour = fh,
      valid_utc     = vdt,
      municipio     = if ("municipio" %in% names(cep_sf)) cep_sf$municipio else NA_character_,
      cep           = cep_sf$cep,
      label         = cep_sf$label,
      lon           = cep_sf$lon,
      lat           = cep_sf$lat,
      accum_mm      = vals,
      stringsAsFactors = FALSE
    )
  }

  df <- dplyr::bind_rows(all_out[!vapply(all_out, is.null, logical(1))])

  df <- df |>
    dplyr::arrange(init_utc, cep, forecast_hour) |>
    dplyr::group_by(init_utc, cep) |>
    dplyr::mutate(
      rain_mm    = dplyr::if_else(dplyr::row_number() == 1L, NA_real_,
                                  pmax(0, accum_mm - dplyr::lag(accum_mm))),
      rain_class = classify_rain(rain_mm)
    ) |>
    dplyr::ungroup()

  df
}

export_spatial <- function(df_multi, run_dt,
                           output_dir = "outputs",
                           formats    = c("geojson_nested", "fgb", "shp"),
                           verbose    = TRUE) {

  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  stem    <- sprintf("wrf_cep_forecast_%s", format(run_dt, "%Y%m%d%H"))
  written <- character(0)

  df_base <- df_multi |>
    dplyr::filter(!is.na(rain_mm)) |>
    dplyr::mutate(
      lon           = round(lon, 5),
      lat           = round(lat, 5),
      rain_mm       = round(rain_mm,   3),
      accum_mm      = round(accum_mm,  3),
      forecast_hour = as.integer(forecast_hour),
      valid_utc_str = format(valid_utc, "%Y-%m-%dT%H:%M:%SZ"),
      init_utc_str  = format(init_utc,  "%Y-%m-%dT%H:%M:%SZ")
    ) |>
    dplyr::select(
      municipio, cep, label, lon, lat,
      init_utc      = init_utc_str,
      valid_utc     = valid_utc_str,
      forecast_hour, accum_mm, rain_mm, rain_class
    )

  n_cep   <- length(unique(df_base$cep))
  n_hours <- length(unique(df_base$forecast_hour))

  if ("geojson_nested" %in% formats) {
    if (verbose) cat("\nBuilding nested GeoJSON (1 feature per CEP)...\n")

    if (!requireNamespace("jsonlite", quietly = TRUE))
      stop("Package 'jsonlite' required. Install with: install.packages('jsonlite')")

    cep_scalars <- df_base |>
      dplyr::arrange(cep, forecast_hour) |>
      dplyr::group_by(cep) |>
      dplyr::summarise(
        municipio     = dplyr::first(municipio),
        label         = dplyr::first(label),
        lon           = dplyr::first(lon),
        lat           = dplyr::first(lat),
        init_utc      = dplyr::first(init_utc),
        max_rain_mm   = max(rain_mm,       na.rm = TRUE),
        total_rain_mm = round(sum(rain_mm, na.rm = TRUE), 3),
        n_rainy_hours = sum(rain_mm > 0,   na.rm = TRUE),
        .groups = "drop"
      )

    cep_arrays <- df_base |>
      dplyr::arrange(cep, forecast_hour) |>
      dplyr::group_by(cep) |>
      dplyr::summarise(
        hours      = list(forecast_hour),
        valid_utc  = list(valid_utc),
        rain_mm    = list(rain_mm),
        accum_mm   = list(accum_mm),
        rain_class = list(rain_class),
        .groups = "drop"
      )

    cep_meta <- dplyr::left_join(cep_scalars, cep_arrays, by = "cep")

    gj_path <- file.path(output_dir, paste0(stem, "_nested.geojson"))
    if (verbose) cat(sprintf("  Building %d features (vectorised)...\n", nrow(cep_meta)))

    features <- lapply(seq_len(nrow(cep_meta)), function(i) {
      list(
        type     = "Feature",
        geometry = list(
          type        = "Point",
          coordinates = c(cep_meta$lon[i], cep_meta$lat[i])
        ),
        properties = list(
          cep           = cep_meta$cep[i],
          municipio     = cep_meta$municipio[i],
          label         = cep_meta$label[i],
          init_utc      = cep_meta$init_utc[i],
          max_rain_mm   = cep_meta$max_rain_mm[i],
          total_rain_mm = cep_meta$total_rain_mm[i],
          n_rainy_hours = as.integer(cep_meta$n_rainy_hours[i]),
          hours         = cep_meta$hours[[i]],
          valid_utc     = cep_meta$valid_utc[[i]],
          rain_mm       = cep_meta$rain_mm[[i]],
          accum_mm      = cep_meta$accum_mm[[i]],
          rain_class    = cep_meta$rain_class[[i]]
        )
      )
    })

    geojson_obj <- list(
      type     = "FeatureCollection",
      crs      = list(type = "name",
                      properties = list(name = "urn:ogc:def:crs:OGC:1.3:CRS84")),
      features = features
    )

    if (verbose) cat("  Serialising to JSON...\n")
    jsonlite::write_json(geojson_obj, gj_path,
                         auto_unbox = TRUE,
                         digits     = 6,
                         pretty     = FALSE)

    sz <- file.info(gj_path)$size / 1024^2
    written <- c(written, gj_path)
    if (verbose)
      cat(sprintf("  -> %s  |  %d features  |  %.1f MB\n",
                  basename(gj_path), nrow(cep_meta), sz))
  }

  if ("fgb" %in% formats) {
    if (verbose) cat("\nWriting FlatGeobuf (long format, binary)...\n")

    sf_long <- sf::st_as_sf(df_base,
                            coords = c("lon","lat"),
                            crs    = 4326,
                            remove = FALSE)

    fgb_path <- file.path(output_dir, paste0(stem, ".fgb"))
    sf::st_write(sf_long, fgb_path,
                 driver     = "FlatGeobuf",
                 delete_dsn = TRUE,
                 quiet      = TRUE)

    sz <- file.info(fgb_path)$size / 1024^2
    written <- c(written, fgb_path)
    if (verbose)
      cat(sprintf("  -> %s  |  %d features  |  %.1f MB\n",
                  basename(fgb_path), nrow(sf_long), sz))
  }

  if ("shp" %in% formats) {
    if (verbose) cat("\nWriting Shapefile (long format, UTF-8)...\n")

    shp_dir  <- file.path(output_dir, "shp")
    dir.create(shp_dir, showWarnings = FALSE, recursive = TRUE)
    shp_path <- file.path(shp_dir, paste0(stem, ".shp"))

    sf_shp <- sf::st_as_sf(df_base,
                           coords = c("lon","lat"),
                           crs    = 4326,
                           remove = FALSE) |>
      dplyr::rename(
        fcst_hr  = forecast_hour,
        rain_cls = rain_class,
        init_dt  = init_utc,
        valid_dt = valid_utc
      )

    sf::st_write(sf_shp, shp_path,
                 layer_options = "ENCODING=UTF-8",
                 delete_dsn    = TRUE,
                 quiet         = TRUE)
    writeLines("UTF-8", sub("\\.shp$", ".cpg", shp_path))

    sz <- sum(file.info(list.files(shp_dir, full.names = TRUE))$size,
              na.rm = TRUE) / 1024^2
    written <- c(written, shp_path)
    if (verbose)
      cat(sprintf("  -> %s  |  %d features  |  %.1f MB total\n",
                  basename(shp_path), nrow(sf_shp), sz))
  }

  if (verbose) {
    cat(sprintf("\n%d CEPs  x  %d forecast hours  =  %d data points\n",
                n_cep, n_hours, n_cep * n_hours))
    cat("\nRain intensity summary:\n")
    print(table(df_base$rain_class))
    cat("\nFiles written:\n")
    for (f in written)
      cat(sprintf("  %-55s  %.1f MB\n", f,
                  file.info(f)$size / 1024^2))
  }

  invisible(written)
}

plot_rain_map <- function(df_multi, shp_path,
                          n_hours_shown = 12,
                          title         = "WRF hourly rainfall forecast") {

  if (!requireNamespace("viridis", quietly = TRUE))
    stop("Package 'viridis' required. Install with: install.packages('viridis')")

  sc_sf <- sf::st_read(shp_path, quiet = TRUE) |> sf::st_transform(4326)

  hours_use <- sort(unique(df_multi$forecast_hour[!is.na(df_multi$rain_mm)]))
  hours_use <- head(hours_use, n_hours_shown)

  df_plot <- df_multi |>
    dplyr::filter(forecast_hour %in% hours_use, !is.na(rain_mm)) |>
    dplyr::mutate(
      panel_label = sprintf("h%02d  %s",
                            forecast_hour,
                            format(valid_utc, "%d/%m %H:%M"))
    )

  ggplot2::ggplot() +
    ggplot2::geom_sf(data = sc_sf, fill = "grey92", colour = "grey70", linewidth = 0.2) +
    ggplot2::geom_point(data = df_plot,
                        ggplot2::aes(x = lon, y = lat, fill = rain_mm),
                        shape = 21, size = 4, colour = "white", stroke = 0.3) +
    viridis::scale_fill_viridis(option = "plasma", direction = -1, name = "mm/h",
                                limits = c(0, max(df_plot$rain_mm, na.rm=TRUE))) +
    ggplot2::facet_wrap(~panel_label) +
    ggplot2::coord_sf(xlim = range(df_plot$lon) + c(-0.3, 0.3),
                      ylim = range(df_plot$lat) + c(-0.3, 0.3)) +
    ggplot2::labs(title = title,
                  x = NULL, y = NULL) +
    ggplot2::theme_minimal(base_size = 10) +
    ggplot2::theme(
      plot.title    = ggplot2::element_text(face = "bold"),
      strip.text    = ggplot2::element_text(size = 7),
      panel.grid    = ggplot2::element_line(colour = "grey88"),
      legend.position = "right"
    )
}

# =============================================================
# STEP 6 — MAIN: load CEP list, extract all CEPs, export
# =============================================================

if (!file.exists(CEP_LIST_PATH))
  stop(
    "CEP list not found: ", CEP_LIST_PATH,
    "\n  Run 00_build_cep_list.R first to generate it.",
    "\n  It only needs to run once (or when you want finer/coarser resolution)."
  )

cat("\nLoading CEP list from: ", CEP_LIST_PATH, "\n")
cep_df <- readRDS(CEP_LIST_PATH)

cep_df$cep <- normalise_cep(cep_df$cep)
cep_sf  <- sf::st_as_sf(cep_df, coords = c("lon","lat"), crs = 4326, remove = FALSE)

cat(sprintf("  %d geocoded CEP points loaded (%d municipalities)\n",
            nrow(cep_sf), length(unique(cep_df$municipio))))
cat(sprintf("  Sample:\n"))
print(head(cep_df[, intersect(c("municipio","localidade","cep_fmt","lon","lat"), names(cep_df))], 5))

cat("\nExtracting rainfall for all CEPs x all forecast hours...\n")
cat(sprintf("  (%d points x up to %d hours = up to %d extractions)\n",
            nrow(cep_sf),
            FORECAST_END_HR - FORECAST_START_HR,
            nrow(cep_sf) * (FORECAST_END_HR - FORECAST_START_HR)))

df_multi <- extract_rain_multi_cep(run_dir, cep_sf, verbose = TRUE)

cat(sprintf("\nExtraction complete: %d rows (%d CEPs x %d hours with data)\n",
            nrow(df_multi[!is.na(df_multi$rain_mm),]),
            length(unique(df_multi$cep)),
            length(unique(df_multi$forecast_hour[!is.na(df_multi$rain_mm)]))))

gc()
cat("\nExporting spatial files...\n")
export_spatial(
  df_multi   = df_multi,
  run_dt     = run_dt,
  output_dir = "outputs",
  formats    = EXPORT_FORMATS,
  verbose    = TRUE
)

p_map <- plot_rain_map(
  df_multi      = df_multi,
  shp_path      = SHP_PATH,
  n_hours_shown = min(12L, FORECAST_END_HR - FORECAST_START_HR),
  title         = sprintf("WRF rainfall forecast — SC municipalities — %s UTC",
                          format(run_dt, "%Y-%m-%d %H:%M"))
)
if (GHA_MODE) {
  plot_path <- file.path("outputs", sprintf("forecast_map_%s.png", format(run_dt, "%Y%m%d%H")))
  ggplot2::ggsave(plot_path, p_map, width = 14, height = 10, dpi = 150, bg = "white")
  message("Map saved to: ", plot_path)
} else {
  print(p_map)
}

# =============================================================
# STEP 7 — Lovable artifacts (UNCHANGED)
# =============================================================

`%||%` <- function(a, b) if (!is.null(a)) a else b

compute_heat_weight <- function(rain_mm) {
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
        diffs <- abs(as.numeric(difftime(ts, nowUtc, units = "secs")))
        i_now <- which.min(diffs)
        rainNow <- rr[i_now]
        nowTsBrt <- paste0(format(ts[i_now], tz = "America/Sao_Paulo", "%d/%m %H:%M"), " BRT")

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

  latest <- list(
    run_id = run_id,
    cycle  = run_id,
    generated_at = generated_at,
    geojson_url = sprintf("outputs/wrf_cep_forecast_%s_nested.geojson", run_id),
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

  write_points_lite_files(nested_path, run_dt, out_dir = "outputs", windows = c(12L, 24L, 48L, 72L))
  write_available_runs(run_dt, out_path = "outputs/available_runs.json")
  write_latest_json(run_dt, out_path = "outputs/latest.json")

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

# =============================================================
# STEP 8 — GRID EXPORT (hourly rain + temp/wind) + 4 HTML files
# =============================================================

read_grid_stack_vars <- function(run_dir, verbose = TRUE) {
  meta <- discover_wrf_files(run_dir)
  if (!nrow(meta)) stop("No files discovered under: ", run_dir)

  r0 <- terra::rast(meta$file[1])
  if (terra::nlyr(r0) < 3 || !all(c("precip_acc_mm","t2m_k","wind_gust_ms") %in% names(r0))) {
    stop("Grid export requires multi-layer *_vars.tif files with names: ",
         "precip_acc_mm, t2m_k, wind_gust_ms. ",
         "Re-run crop step; ensure *_vars.tif exist.")
  }

  ref <- r0[["precip_acc_mm"]]
  if (!terra::is.lonlat(ref)) ref <- terra::project(ref, "EPSG:4326")

  xy <- terra::xyFromCell(ref, seq_len(terra::ncell(ref)))
  coords <- data.frame(
    cell_id = seq_len(nrow(xy)),
    lon = round(xy[,1], 5),
    lat = round(xy[,2], 5),
    stringsAsFactors = FALSE
  )

  n_cells <- nrow(coords)
  n_steps <- nrow(meta)

  acc <- matrix(NA_real_, n_cells, n_steps)
  t2k <- matrix(NA_real_, n_cells, n_steps)
  wnd <- matrix(NA_real_, n_cells, n_steps)

  for (i in seq_len(n_steps)) {
    if (verbose) message(sprintf("  grid [%d/%d] h%03d %s (%s)",
                                 i, n_steps, meta$forecast_hour[i], basename(meta$file[i]), meta$valid_brt_lbl[i]))
    rr <- terra::rast(meta$file[i])

    p <- rr[["precip_acc_mm"]]
    t <- rr[["t2m_k"]]
    w <- rr[["wind_gust_ms"]]

    if (!terra::is.lonlat(p)) p <- terra::project(p, "EPSG:4326")
    if (!terra::is.lonlat(t)) t <- terra::project(t, "EPSG:4326")
    if (!terra::is.lonlat(w)) w <- terra::project(w, "EPSG:4326")

    if (!isTRUE(all.equal(terra::ext(p), terra::ext(ref))) || !isTRUE(all.equal(terra::res(p), terra::res(ref))))
      p <- terra::resample(p, ref, method = "bilinear")
    if (!isTRUE(all.equal(terra::ext(t), terra::ext(ref))) || !isTRUE(all.equal(terra::res(t), terra::res(ref))))
      t <- terra::resample(t, ref, method = "bilinear")
    if (!isTRUE(all.equal(terra::ext(w), terra::ext(ref))) || !isTRUE(all.equal(terra::res(w), terra::res(ref))))
      w <- terra::resample(w, ref, method = "bilinear")

    acc[, i] <- as.numeric(terra::values(p))
    t2k[, i] <- as.numeric(terra::values(t))
    wnd[, i] <- as.numeric(terra::values(w))
  }

  list(coords = coords, meta = meta, rain_acc = acc, t2m_k = t2k, wind_ms = wnd)
}

grid_hourly_rain <- function(meta, acc) {
  n_cells <- nrow(acc)
  n_steps <- ncol(acc)
  out <- matrix(NA_real_, n_cells, n_steps)

  for (init in unique(meta$init_utc)) {
    idx <- which(meta$init_utc == init)
    idx <- idx[order(meta$forecast_hour[idx])]
    if (length(idx) < 2) next
    for (j in seq(2, length(idx))) {
      out[, idx[j]] <- pmax(0, acc[, idx[j]] - acc[, idx[j-1]])
    }
  }
  out
}

grid_to_long_hourly <- function(st) {
  coords <- st$coords; meta <- st$meta
  acc <- st$rain_acc; t2k <- st$t2m_k; wnd <- st$wind_ms

  rain_hr <- grid_hourly_rain(meta, acc)

  temp_c <- t2k
  okK <- !is.na(t2k) & t2k > 100
  temp_c[okK] <- t2k[okK] - 273.15

  n_cells <- nrow(coords); n_steps <- nrow(meta)
  rows <- vector("list", n_cells * n_steps)
  k <- 0L
  for (ci in seq_len(n_cells)) for (j in seq_len(n_steps)) {
    k <- k + 1L
    rows[[k]] <- data.frame(
      cell_id = coords$cell_id[ci],
      lon = coords$lon[ci],
      lat = coords$lat[ci],
      init_utc = fmt_utc_iso(meta$init_utc[j]),
      valid_utc = fmt_utc_iso(meta$valid_utc[j]),
      valid_brt = fmt_brt_iso(meta$valid_utc[j]),
      valid_brt_lbl = meta$valid_brt_lbl[j],
      forecast_hour = as.integer(meta$forecast_hour[j]),
      accum_mm = round(acc[ci, j], 3),
      rain_mm  = round(rain_hr[ci, j], 3),
      temp_c   = round(temp_c[ci, j], 2),
      wind_ms  = round(wnd[ci, j], 2),
      stringsAsFactors = FALSE
    )
  }
  df <- dplyr::bind_rows(rows)
  df$rain_class <- classify_rain(df$rain_mm)
  df
}

export_grid_nested_json_rain <- function(st, run_dt, out_path, max_mb = 95, verbose = TRUE) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Package 'jsonlite' required.")

  coords <- st$coords; meta <- st$meta; acc <- st$rain_acc
  rain_hr <- grid_hourly_rain(meta, acc)

  hours_vec <- as.integer(meta$forecast_hour)
  valid_utc <- vapply(meta$valid_utc, fmt_utc_iso, character(1))
  valid_brt <- vapply(meta$valid_utc, fmt_brt_iso, character(1))
  valid_lbl <- meta$valid_brt_lbl

  sel <- which(rowSums(!is.na(rain_hr)) > 0)

  cells <- lapply(sel, function(ci) {
    rr <- round(rain_hr[ci, ], 3)
    aa <- round(acc[ci, ], 3)
    ok <- !is.na(rr)
    list(
      cell_id = as.integer(coords$cell_id[ci]),
      lon = coords$lon[ci],
      lat = coords$lat[ci],
      max_rain_mm = round(max(rr[ok], na.rm = TRUE), 3),
      total_rain_mm = round(sum(rr[ok], na.rm = TRUE), 3),
      hours = hours_vec[ok],
      valid_utc = valid_utc[ok],
      valid_brt = valid_brt[ok],
      valid_brt_lbl = valid_lbl[ok],
      rain_mm = rr[ok],
      accum_mm = aa[ok],
      rain_class = classify_rain(rr[ok])
    )
  })

  obj <- list(
    meta = list(
      run_id = format(run_dt, "%Y%m%d%H"),
      init_utc = fmt_utc_iso(run_dt),
      init_brt = fmt_brt_iso(run_dt),
      init_brt_lbl = fmt_brt_label(run_dt),
      tz_display = BRT_TZ,
      n_cells = length(sel),
      n_steps = length(hours_vec)
    ),
    cells = cells
  )

  jsonlite::write_json(obj, out_path, auto_unbox = TRUE, digits = 6, pretty = FALSE)
  sz <- file.info(out_path)$size / 1024^2
  if (verbose) cat(sprintf("  -> %s | %.1f MB\n", basename(out_path), sz))

  if (sz > max_mb) {
    warning(sprintf("Grid JSON %.1f MB > %.0f MB; removing dry cells (max_rain_mm==0).", sz, max_mb))
    cells2 <- Filter(function(x) isTRUE(x$max_rain_mm > 0), cells)
    obj$cells <- cells2
    obj$meta$n_cells <- length(cells2)
    obj$meta$dry_cells_removed <- TRUE
    jsonlite::write_json(obj, out_path, auto_unbox = TRUE, digits = 6, pretty = FALSE)
    sz2 <- file.info(out_path)$size / 1024^2
    if (verbose) cat(sprintf("  -> rewritten | %.1f MB\n", sz2))
  }

  invisible(out_path)
}

build_leaflet_html <- function(df_grid, run_dt, out_path,
                               mode = c("tabs", "rain", "temp", "wind"),
                               verbose = TRUE) {
  mode <- match.arg(mode)

  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Package 'jsonlite' required.")
  if (!requireNamespace("glue", quietly = TRUE)) stop("Package 'glue' required (add to .github/r-packages.txt).")

  hours <- sort(unique(df_grid$forecast_hour))
  hour_data <- lapply(hours, function(h) {
    sub <- dplyr::filter(df_grid, forecast_hour == h)
    list(
      hour = h,
      label_brt = if (nrow(sub)) sub$valid_brt_lbl[1] else sprintf("h%02d", h),
      lon = sub$lon, lat = sub$lat,
      rain_mm = sub$rain_mm,
      temp_c  = sub$temp_c,
      wind_ms = sub$wind_ms
    )
  })
  data_json <- jsonlite::toJSON(hour_data, auto_unbox = TRUE, digits = 4, na = "null")

  map_lon <- mean(df_grid$lon, na.rm = TRUE)
  map_lat <- mean(df_grid$lat, na.rm = TRUE)

  run_id <- format(run_dt, "%Y%m%d%H")
  init_brt <- fmt_brt_label(run_dt)

  init_var <- if (mode == "tabs") "rain" else mode
  show_tabs <- mode == "tabs"

  tabs_html <- if (show_tabs) {
    '
  <div class="tabs">
    <button class="btn active" onclick="setVar(\'rain\', this)">Chuva (mm/h)</button>
    <button class="btn" onclick="setVar(\'temp\', this)">Temp (°C)</button>
    <button class="btn" onclick="setVar(\'wind\', this)">Vento (m/s)</button>
  </div>'
  } else {
    sprintf('<div class="meta">Variável: <b>%s</b></div>', init_var)
  }

  title_suffix <- switch(mode,
                         tabs = "rain/temp/wind",
                         rain = "rain",
                         temp = "temp",
                         wind = "wind"
  )

  html <- glue::glue(
    '<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>WRF SC Grid — {run_id} — {title_suffix}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
<style>
  * {{ box-sizing: border-box; margin:0; padding:0; }}
  body {{ font-family: Arial, sans-serif; background:#0b1220; color:#e5e7eb; height:100vh; display:flex; flex-direction:column; }}
  #top {{ padding:10px 14px; background:#0f172a; border-bottom:1px solid #1f2937; display:flex; gap:14px; flex-wrap:wrap; align-items:center; }}
  #top .title {{ font-weight:700; font-size:14px; }}
  #top .meta {{ font-size:12px; color:#9ca3af; }}
  #controls {{ padding:10px 14px; background:#0f172a; border-bottom:1px solid #1f2937; display:flex; gap:16px; align-items:center; flex-wrap:wrap; }}
  .tabs {{ display:flex; gap:6px; }}
  .btn {{ border:1px solid #334155; background:transparent; color:#cbd5e1; padding:5px 12px; border-radius:999px; cursor:pointer; font-size:12px; }}
  .btn.active {{ background:#3b82f6; border-color:#3b82f6; color:white; }}
  .slider {{ display:flex; gap:10px; align-items:center; flex:1; min-width:220px; }}
  #map {{ flex:1; }}
  #hourLabel {{ font-weight:700; min-width:140px; }}
  #legend {{ position:absolute; right:10px; bottom:20px; z-index:1000; background:rgba(15,23,42,0.92); border:1px solid #1f2937; border-radius:10px; padding:10px 12px; font-size:12px; min-width:150px; }}
</style>
</head>
<body>
<div id="top">
  <div class="title">WRF/CPTEC — SC — Grade nativa (7 km)</div>
  <div class="meta">Rodada: <b>{init_brt}</b> (Horário de Brasília)</div>
</div>

<div id="controls">
  {tabs_html}
  <div class="slider">
    <span>Hora:</span>
    <input id="hour" type="range" min="0" max="0" value="0" step="1" oninput="setHour(this.value)" style="flex:1"/>
    <span id="hourLabel">—</span>
  </div>
</div>

<div id="map"></div>
<div id="legend"><div id="legTitle"></div><div id="legBody"></div></div>

<script>
const DATA = {data_json};

let currentVar = "{init_var}";
let idx = 0;
let layerPts = null;
let layerHeat = null;

const map = L.map("map").setView([{round(map_lat,4)}, {round(map_lon,4)}], 8);
L.tileLayer("https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png",
  {{attribution:"&copy; OpenStreetMap &copy; CARTO", maxZoom: 18}}).addTo(map);

function rainColor(v) {{
  if (v === null || isNaN(v)) return "#777";
  if (v <= 0) return "#c6e2ff";
  if (v < 2.5) return "#4fc3f7";
  if (v < 7.5) return "#1976d2";
  if (v < 35) return "#7b1fa2";
  return "#b71c1c";
}}
function tempColor(v) {{
  if (v === null || isNaN(v)) return "#777";
  if (v < 0) return "#440154";
  if (v < 10) return "#31688e";
  if (v < 20) return "#35b779";
  if (v < 30) return "#fde725";
  return "#ff0000";
}}
function windColor(v) {{
  if (v === null || isNaN(v)) return "#777";
  if (v < 5) return "#b2dfdb";
  if (v < 10) return "#26a69a";
  if (v < 15) return "#f9a825";
  if (v < 25) return "#e65100";
  return "#b71c1c";
}}

function setLegend() {{
  const t = document.getElementById("legTitle");
  const b = document.getElementById("legBody");
  if (currentVar === "rain") {{
    t.textContent = "mm/h";
    b.innerHTML = "<div>0: none</div><div>0–2.5: light</div><div>2.5–7.5: moderate</div><div>7.5–35: heavy</div><div>≥35: extreme</div>";
  }} else if (currentVar === "temp") {{
    t.textContent = "°C";
    b.innerHTML = "<div>Roxo (frio) → Vermelho (quente)</div>";
  }} else {{
    t.textContent = "m/s";
    b.innerHTML = "<div>Verde (fraco) → Vermelho (forte)</div>";
  }}
}}

function render() {{
  if (layerPts) map.removeLayer(layerPts);
  if (layerHeat) map.removeLayer(layerHeat);

  const d = DATA[idx];
  document.getElementById("hourLabel").textContent = d.label_brt;
  setLegend();

  if (currentVar === "rain") {{
    const heat = [];
    const pts = [];
    for (let i=0; i<d.lat.length; i++) {{
      const v = d.rain_mm[i];
      if (v !== null && !isNaN(v) && v > 0) heat.push([d.lat[i], d.lon[i], Math.min(v/35, 1)]);
      if (v !== null && !isNaN(v)) pts.push(
        L.circleMarker([d.lat[i], d.lon[i]], {{radius:4, color:"transparent", fillColor:rainColor(v), fillOpacity:0.65, weight:0}})
        .bindTooltip(`${v.toFixed(2)} mm/h<br>${d.label_brt}`, {{sticky:true}})
      );
    }}
    if (heat.length && typeof L.heatLayer !== "undefined") {{
      layerHeat = L.heatLayer(heat, {{radius:18, blur:15, maxZoom:12}}).addTo(map);
    }}
    layerPts = L.layerGroup(pts).addTo(map);
  }} else if (currentVar === "temp") {{
    const pts = [];
    for (let i=0; i<d.lat.length; i++) {{
      const v = d.temp_c[i];
      if (v === null || isNaN(v)) continue;
      pts.push(L.circleMarker([d.lat[i], d.lon[i]], {{radius:5, color:"transparent", fillColor:tempColor(v), fillOpacity:0.75, weight:0}})
        .bindTooltip(`${v.toFixed(1)} °C<br>${d.label_brt}`, {{sticky:true}}));
    }}
    layerPts = L.layerGroup(pts).addTo(map);
  }} else {{
    const pts = [];
    for (let i=0; i<d.lat.length; i++) {{
      const v = d.wind_ms[i];
      if (v === null || isNaN(v)) continue;
      pts.push(L.circleMarker([d.lat[i], d.lon[i]], {{radius:5, color:"transparent", fillColor:windColor(v), fillOpacity:0.75, weight:0}})
        .bindTooltip(`${v.toFixed(1)} m/s<br>${d.label_brt}`, {{sticky:true}}));
    }}
    layerPts = L.layerGroup(pts).addTo(map);
  }}
}}

function setHour(v) {{
  idx = parseInt(v);
  render();
}}

function setVar(v, el) {{
  currentVar = v;
  if (el) {{
    document.querySelectorAll(".btn").forEach(x=>x.classList.remove("active"));
    el.classList.add("active");
  }}
  render();
}}

document.getElementById("hour").max = DATA.length - 1;
render();
</script>
</body>
</html>'
  )

  writeLines(html, out_path, useBytes = FALSE)
  if (verbose) cat(sprintf("  -> %s | %.1f MB\n", basename(out_path), file.info(out_path)$size/1024^2))
  invisible(out_path)
}

export_grid <- function(run_dir, run_dt, output_dir = "outputs",
                        export_json = TRUE, export_shp = TRUE, export_html = TRUE,
                        max_json_mb = 95, verbose = TRUE) {
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  stem <- sprintf("wrf_grid_forecast_%s", format(run_dt, "%Y%m%d%H"))

  cat("\n=== STEP 8: Native WRF Grid Export ===\n")
  st <- read_grid_stack_vars(run_dir, verbose = verbose)
  df_long <- grid_to_long_hourly(st)

  if (export_json) {
    out_json <- file.path(output_dir, paste0(stem, "_nested.json"))
    export_grid_nested_json_rain(st, run_dt, out_json, max_mb = max_json_mb, verbose = verbose)
  }

  if (export_shp) {
    shp_dir <- file.path(output_dir, "shp_grid")
    dir.create(shp_dir, showWarnings = FALSE, recursive = TRUE)
    shp_file <- file.path(shp_dir, paste0(stem, ".shp"))

    sf_grid <- sf::st_as_sf(df_long, coords = c("lon", "lat"), crs = 4326, remove = FALSE) |>
      dplyr::rename(fcst_hr = forecast_hour, rain_cls = rain_class, v_brt = valid_brt)

    sf::st_write(sf_grid, dsn = shp_file, driver = "ESRI Shapefile",
                 layer_options = "ENCODING=UTF-8", delete_dsn = TRUE, quiet = TRUE)
    writeLines("UTF-8", sub("\\.shp$", ".cpg", shp_file))
    cat(sprintf("  -> %s | %.1f MB\n", basename(shp_file),
                sum(file.info(list.files(shp_dir, pattern = paste0(stem, "\\."), full.names = TRUE))$size, na.rm = TRUE)/1024^2))
  }

  if (export_html) {
    run_id <- format(run_dt, "%Y%m%d%H")
    out_tabs <- file.path(output_dir, sprintf("forecast_map_%s.html", run_id))
    out_rain <- file.path(output_dir, sprintf("forecast_map_rain_%s.html", run_id))
    out_temp <- file.path(output_dir, sprintf("forecast_map_temp_%s.html", run_id))
    out_wind <- file.path(output_dir, sprintf("forecast_map_wind_%s.html", run_id))

    build_leaflet_html(df_long, run_dt, out_tabs, mode = "tabs", verbose = verbose)
    build_leaflet_html(df_long, run_dt, out_rain, mode = "rain", verbose = verbose)
    build_leaflet_html(df_long, run_dt, out_temp, mode = "temp", verbose = verbose)
    build_leaflet_html(df_long, run_dt, out_wind, mode = "wind", verbose = verbose)
  }

  invisible(TRUE)
}

if (GRID_EXPORT_JSON || GRID_EXPORT_SHP || GRID_EXPORT_HTML) {
  export_grid(run_dir, run_dt,
              output_dir  = "outputs",
              export_json = GRID_EXPORT_JSON,
              export_shp  = GRID_EXPORT_SHP,
              export_html = GRID_EXPORT_HTML,
              max_json_mb = MAX_GRID_JSON_MB,
              verbose     = TRUE)
}
