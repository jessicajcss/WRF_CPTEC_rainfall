###############################################################
# WRF / GRIB2 – hourly rainfall forecast
#
#  Step 0 : Configuration
#  Step 1 : Download  (original working downloader, unchanged)
#  Step 2 : Crop & delete (clip GRIB2 to shapefile bbox → .tif, delete GRIB2)
#  Step 3 : Discover files & parse metadata
#  Step 4 : Extract precip layer + compute TRUE hourly rain
#  Step 5 : Plot by MUNICIPALITY (polygon mean) or by CEP (point)
#
# Memory strategy
# ---------------
#  Full WRF GRIB2 covers all of South America (~20 MB each).
#  After download we immediately:
#    (a) crop to the shapefile bounding box + buffer
#    (b) save as a small .tif  (typically < 200 KB for SC)
#    (c) delete the original GRIB2
#  All subsequent reads use the tiny .tif files.
#
# Mathematical rationale
# ----------------------
#  WRF "Total precipitation" in GRIB2 is ACCUMULATED (mm) since
#  model initialisation (t=0).  Converting to mm/hour:
#
#    rain_mm[h] = accum[h] - accum[h-1]          (within one run)
#
#  Rules that make this correct:
#   (a) Group by init_utc BEFORE differencing — accumulation resets
#       to 0 at each new init, so cross-run diffs produce nonsense.
#   (b) h=0 (first step) is set to NA  (no prior step exists).
#   (c) Negative diffs (numerical noise) are clamped to 0.
#   (d) kg/m^2 == mm for liquid water (density 1000 kg/m^3).
#
# Requires: terra, sf, dplyr, ggplot2, lubridate, stringr,
#           rvest, httr, future, furrr, geocodebr, progressr
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
# STEP 0 — USER CONFIGURATION  (edit this block only)
# =============================================================

## ---- 0a) Desired output mode ----
# "municipality"  -> polygon mean over a Brazilian municipality
# "cep"           -> nearest grid cell to a Brazilian CEP (postcode)
OUTPUT_MODE <- "municipality"   # or "cep"

## ---- 0b) Target location ----
MUNICIPIO_NAME <- "Joinville"   # used when OUTPUT_MODE == "municipality"
CEP            <- "89201-100"   # used when OUTPUT_MODE == "cep"
# any format works: "89201-100", "89201100", 89201100

## ---- 0c) Shapefile (municipalities) ----
# Also used as the crop bounding box, so both modes benefit from memory savings.
SHP_PATH <- "./data/shp/SC_Municipios_2024/SC_Municipios_2024.shp"

## ---- 0d) WRF run ----
# Explicit string "YYYY-MM-DD HH:MM" or NULL to auto-detect latest 00Z/12Z.
RUN_START <- NULL   # e.g. "2026-03-04 00:00"
# GHA: override with env var set by workflow_dispatch input
if (GHA_MODE && nzchar(Sys.getenv("WRF_RUN_START")))
  RUN_START <- Sys.getenv("WRF_RUN_START")

## ---- 0e) Forecast window ----
FORECAST_START_HR <- 0
FORECAST_END_HR   <- 72
# GHA: override with env var
if (GHA_MODE && nzchar(Sys.getenv("WRF_FORECAST_END_HR")))
  FORECAST_END_HR <- as.integer(Sys.getenv("WRF_FORECAST_END_HR"))

## ---- 0f) Paths & workers ----
# Files land at: OUTPUT_DIR/<YYYYMMDD>/hXXX/<filename>.tif
OUTPUT_DIR <- "data/WRF_downloads"
N_WORKERS  <- if (GHA_MODE) 2L else min(8L, max(1L, parallel::detectCores(logical=FALSE)))

## ---- 0g) Pipeline switches ----
# Path to the pre-built CEP list produced by 00_build_cep_list.R
# Run that script once; afterwards this loads in milliseconds.
CEP_LIST_PATH <- "outputs/cep_list.rds"

## ---- 0h) Export formats ----
# In GHA only "geojson_nested" is produced (smallest + fastest).
# Add "fgb" or "shp" locally if you also need QGIS/ArcGIS formats.
EXPORT_FORMATS <- if (GHA_MODE) c("geojson_nested") else c("geojson_nested", "fgb", "shp")


SKIP_DOWNLOAD   <- FALSE   # TRUE = files already on disk, skip download
SKIP_CROP       <- FALSE   # TRUE = .tif files already exist, skip crop step
CROP_BUFFER_DEG <- 0.5     # degrees of padding around shapefile bbox

# =============================================================
# STEP 1 — DOWNLOADER
# Original working code preserved exactly.
# Key fix vs previous version: dated_dir uses YYYYMMDD (no hour),
# matching the path structure that was confirmed to work.
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

  # A file is "done" if the GRIB2 OR its cropped .tif already exists
  tif_paths <- sub("\\.grib2$", ".tif", file_df$local_path, ignore.case=TRUE)
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
      message(sprintf("Auto-detected latest run: %s UTC", format(run_dt,"%Y-%m-%d %H:%M")))
      return(run_dt)
    }
  }
  stop("Could not find a reachable WRF run. Set RUN_START manually.")
}


# =============================================================
# STEP 2 — CROP & DELETE
# After downloading, immediately:
#   1. Read the precip layer from the full-domain GRIB2
#   2. Crop to the shapefile bounding box + CROP_BUFFER_DEG
#   3. Save as LZW-compressed .tif  (same filename stem, same folder)
#   4. Delete the original .grib2
#
# Result: ~20 MB GRIB2 becomes ~100-200 KB .tif for SC extent.
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

get_crop_extent <- function(shp_path, buffer_deg=0.5) {
  v   <- terra::vect(shp_path)
  ext <- terra::ext(v)
  terra::ext(ext$xmin - buffer_deg, ext$xmax + buffer_deg,
             ext$ymin - buffer_deg, ext$ymax + buffer_deg)
}

crop_and_save_one <- function(grib2_path, crop_ext_lonlat, verbose=FALSE) {
  tif_path <- sub("\\.grib2$", ".tif", grib2_path, ignore.case=TRUE)

  if (file.exists(tif_path) && !file.exists(grib2_path)) return(tif_path)  # already done
  if (file.exists(tif_path) && file.exists(grib2_path)) {
    file.remove(grib2_path); return(tif_path)                               # tif ok, clean up
  }

  tryCatch({
    r <- terra::rast(grib2_path)
    p <- pick_precip_layer(r, verbose=verbose)

    # Reproject crop window to raster CRS when needed
    crop_ext_use <- if (!terra::is.lonlat(p)) {
      pts <- terra::vect(
        data.frame(x=c(crop_ext_lonlat$xmin, crop_ext_lonlat$xmax,
                       crop_ext_lonlat$xmin, crop_ext_lonlat$xmax),
                   y=c(crop_ext_lonlat$ymin, crop_ext_lonlat$ymin,
                       crop_ext_lonlat$ymax, crop_ext_lonlat$ymax)),
        geom=c("x","y"), crs="EPSG:4326")
      terra::ext(terra::project(pts, terra::crs(p)))
    } else { crop_ext_lonlat }

    p_crop <- terra::crop(p, crop_ext_use)
    terra::writeRaster(p_crop, tif_path, overwrite=TRUE, gdal=c("COMPRESS=LZW"))
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
    cat(sprintf("\nCropping %d GRIB2 files to shapefile bbox + %.1f deg buffer...\n",
                length(grib2_files), buffer_deg))
    cat(sprintf("  Extent: %.2fW to %.2fE, %.2fS to %.2fN\n",
                abs(crop_ext$xmin), abs(crop_ext$xmax),
                abs(crop_ext$ymin), abs(crop_ext$ymax)))
  }

  results <- vapply(seq_along(grib2_files), function(i) {
    f <- grib2_files[i]
    if (verbose) message(sprintf("  crop [%d/%d] %s", i, length(grib2_files), basename(f)))
    crop_and_save_one(f, crop_ext, verbose=FALSE)
  }, character(1))

  tif_ok     <- results[!is.na(results)]
  size_after <- sum(file.info(tif_ok)$size, na.rm=TRUE) / 1024^2

  if (verbose) {
    cat(sprintf("\nCrop complete: %d / %d files converted to .tif\n",
                length(tif_ok), length(grib2_files)))
    cat(sprintf("Disk usage: %.1f MB (GRIB2)  ->  %.1f MB (.tif)  [%.0f%% saved]\n\n",
                size_before, size_after, 100*(1 - size_after/max(size_before,1))))
  }
  invisible(results)
}


# =============================================================
# STEP 3 — FILE DISCOVERY & METADATA
# Finds .tif (post-crop) first; falls back to .grib2
# =============================================================

discover_wrf_files <- function(run_dir) {
  files <- list.files(run_dir, pattern="\\.tif$",   full.names=TRUE, recursive=TRUE)
  ext_used <- "tif"
  if (!length(files)) {
    files    <- list.files(run_dir, pattern="\\.grib2$", full.names=TRUE, recursive=TRUE)
    ext_used <- "grib2"
  }
  if (!length(files)) stop("No .tif or .grib2 files found under: ", run_dir)
  message(sprintf("Discovered %d .%s files.", length(files), ext_used))

  base      <- tools::file_path_sans_ext(basename(files))
  p1        <- "^WRF_cpt_07KM_(\\d{10})_(\\d{10})$"
  p2        <- "^WRF_cpt_07KM_(\\d{10})$"
  is_p1     <- grepl(p1, base)
  is_p2     <- grepl(p2, base) & !is_p1
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
    valid_utc     = valid_utc
  ) |> dplyr::arrange(init_utc, forecast_hour)
}


# =============================================================
# STEP 4 — EXTRACT & CONVERT TO TRUE HOURLY RAIN
# =============================================================

extract_one_file <- function(file, target_vect, fun=mean, verbose=FALSE) {
  if (!file.exists(file))          { warning("Not found: ", file);          return(NA_real_) }
  if (file.info(file)$size < 100L) { warning("Too small: ", basename(file)); return(NA_real_) }

  tryCatch({
    r <- terra::rast(file)
    # .tif files have exactly 1 layer (the precip layer we saved);
    # raw .grib2 files have many layers and need layer selection.
    p <- if (terra::nlyr(r) == 1) r else pick_precip_layer(r, verbose=verbose)

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
    if (verbose) message(sprintf("  [%d/%d] h%03d  %s",
                                 i, nrow(meta), meta$forecast_hour[i], basename(meta$file[i])))
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

  # Normalise CEP to 8-digit string regardless of input format
  cep_clean <- normalise_cep(cep)
  if (nchar(cep_clean) != 8 || is.na(suppressWarnings(as.integer(cep_clean))))
    stop("Invalid CEP: ", cep, " -> normalised to '", cep_clean, "'")

  cep_fmt <- paste0(substr(cep_clean,1,5), "-", substr(cep_clean,6,8))

  # Try to get coordinates from pre-built cep_list first (fast, offline)
  lon <- NA_real_; lat <- NA_real_
  if (!is.null(cep_list_df)) {
    # cep_list stores CEP as 8-digit string — normalise both sides
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

  # Fall back to geocodebr if not found in cep_list
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
    ggplot2::geom_col(fill="#2166ac", width=3600 * 0.9) +  # width in seconds for datetime x-axis
    ggplot2::scale_x_datetime(date_labels="%d/%m\n%H:%M", date_breaks="6 hours") +
    ggplot2::labs(title=title, subtitle=subtitle,
                  x="Forecast valid time (UTC)", y="Rainfall (mm / hour)") +
    ggplot2::theme_minimal(base_size=13) +
    ggplot2::theme(
      plot.title       = ggplot2::element_text(face="bold"),
      plot.subtitle    = ggplot2::element_text(color="grey40"),
      panel.grid.minor = ggplot2::element_blank()
    )
  if (identical(Sys.getenv("GHA_MODE"), "true")) {
    invisible(p)  # don't print to Rplots.pdf in headless mode
  } else {
    print(p)
  }
  invisible(p)
}

# Interactive diagnostic for a single file
diagnose_grib2 <- function(file, target_vect=NULL) {
  cat("File   :", file, "\n")
  cat("Exists :", file.exists(file), "\n")
  cat("Size   :", round(file.info(file)$size/1024, 1), "KB\n")
  r <- tryCatch(terra::rast(file), error=function(e){cat("rast() ERROR:",e$message,"\n"); NULL})
  if (is.null(r)) return(invisible(NULL))
  cat("Layers :", terra::nlyr(r), "\n")
  cat("Names  :\n"); print(names(r))
  cat("CRS    :", terra::crs(r, describe=TRUE)$name, "\n")
  p <- tryCatch(pick_precip_layer(r, verbose=TRUE),
                error=function(e){cat("pick ERROR:",e$message,"\n"); NULL})
  if (!is.null(p) && !is.null(target_vect)) {
    if (!terra::same.crs(target_vect, p)) target_vect <- terra::project(target_vect, terra::crs(p))
    v <- tryCatch(terra::extract(p, target_vect, fun=mean, na.rm=TRUE),
                  error=function(e){cat("extract ERROR:",e$message,"\n"); NULL})
    if (!is.null(v)) { cat("Extract result:\n"); print(v) }
  }
  invisible(r)
}


# =============================================================
# MAIN EXECUTION
# =============================================================

## ---- Resolve run datetime ----
run_dt <- if (is.null(RUN_START)) detect_latest_run() else
  lubridate::ymd_hm(RUN_START, tz="UTC")

# run_dir = OUTPUT_DIR/YYYYMMDD/  (matches the downloader's dated_dir)
run_dir <- file.path(OUTPUT_DIR, format(run_dt, "%Y%m%d"))

## ---- Step 1: Download ----
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

gc()  # free memory held by download workers before crop step
## ---- Step 2: Crop GRIB2 to shapefile bbox, save .tif, delete GRIB2 ----
if (!SKIP_CROP) {
  crop_run_directory(run_dir, SHP_PATH,
                     buffer_deg = CROP_BUFFER_DEG,
                     verbose    = TRUE)
} else {
  message("SKIP_CROP = TRUE  —  expecting .tif files already in: ", run_dir)
}

## ---- Steps 3-4: Discover + Extract ----
if (OUTPUT_MODE == "municipality") {
  df <- extract_rain_municipality(run_dir, SHP_PATH, MUNICIPIO_NAME, verbose=TRUE)
} else if (OUTPUT_MODE == "cep") {
  # Load cep_list for fast coordinate lookup (falls back to geocodebr if not found)
  cep_list_df <- if (file.exists(CEP_LIST_PATH)) readRDS(CEP_LIST_PATH) else NULL
  if (is.null(cep_list_df))
    message("cep_list.rds not found — will geocode via geocodebr (slower).")
  df <- extract_rain_cep(run_dir, CEP, cep_list_df = cep_list_df, verbose = TRUE)
} else {
  stop("OUTPUT_MODE must be 'municipality' or 'cep'. Got: ", OUTPUT_MODE)
}

## ---- Step 5: Plot ----
plot_rain_bar(
  df,
  title    = sprintf("%s — WRF hourly rainfall forecast",
                     if (OUTPUT_MODE=="municipality") MUNICIPIO_NAME else paste0("CEP ", CEP)),
  subtitle = sprintf("Run init: %s UTC  |  %s",
                     format(run_dt, "%Y-%m-%d %H:%M"),
                     if (OUTPUT_MODE=="municipality") "mean over municipality polygon"
                     else "nearest grid cell")
)

## ---- Inspect numbers ----
cat("\nHourly rainfall table (mm):\n")
print(dplyr::filter(df, !is.na(rain_mm)) |>
        dplyr::select(valid_utc, forecast_hour, accum_mm, rain_mm),
      n = Inf)


# =============================================================
# STEP 6 — SPATIAL EXPORT  (GeoJSON + Shapefile)
# =============================================================
#
# Produces two outputs per forecast run:
#
#   outputs/
#     wrf_cep_forecast_YYYYMMDDHH.geojson   ← all hours, all CEPs in one file
#                                              (best for Lovable & R mapping)
#     shp/
#       wrf_cep_forecast_YYYYMMDDHH.shp     ← same data as shapefile for QGIS
#
# Each feature is one CEP × one forecast hour with properties:
#   cep, label, lon, lat, init_utc, valid_utc, forecast_hour,
#   accum_mm, rain_mm, rain_class
#
# rain_class is a human-readable intensity label:
#   "none" / "light" / "moderate" / "heavy" / "extreme"
# (thresholds per WMO guidelines for hourly rainfall)
#
# For Lovable: import the .geojson — each hour is a separate feature,
# filter by `forecast_hour` property to drive a time-slider map.
# =============================================================

# ---- 6a) Rainfall intensity classification (mm/hour) ----
classify_rain <- function(x) {
  dplyr::case_when(
    is.na(x)  ~ "no data",
    x == 0    ~ "none",
    x <  2.5  ~ "light",       # < 2.5 mm/h
    x <  7.5  ~ "moderate",    # 2.5–7.5 mm/h
    x < 35.0  ~ "heavy",       # 7.5–35 mm/h
    TRUE      ~ "extreme"      # >= 35 mm/h
  )
}

# ---- 6b) Geocode a named vector of CEPs → sf point layer ----
#
# cep_list : named character vector, e.g.:
#   c("Centro Joinville" = "89201-100", "Norte Joinville" = "89227-315")
# Names become the "label" column; if unnamed, CEP itself is the label.

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

# ---- 6c) Extract rain for ALL CEPs across all forecast hours ----

extract_rain_multi_cep <- function(run_dir, cep_sf, verbose = TRUE) {
  meta <- discover_wrf_files(run_dir)

  # Convert to terra SpatVector once
  pts_vect <- terra::vect(cep_sf)

  all_out <- vector("list", nrow(meta))

  for (i in seq_len(nrow(meta))) {
    fh  <- meta$forecast_hour[i]
    vdt <- meta$valid_utc[i]
    if (verbose) message(sprintf("  [%d/%d] h%03d  %s", i, nrow(meta), fh, basename(meta$file[i])))

    f <- meta$file[i]
    if (!file.exists(f) || file.info(f)$size < 100L) {
      warning("Skipping missing/small file: ", basename(f))
      next
    }

    vals <- tryCatch({
      r <- terra::rast(f)
      p <- if (terra::nlyr(r) == 1) r else pick_precip_layer(r, verbose = FALSE)
      pts2 <- if (!terra::same.crs(pts_vect, p))
        terra::project(pts_vect, terra::crs(p)) else pts_vect
      v <- terra::extract(p, pts2, fun = mean, na.rm = TRUE)
      as.numeric(v[, 2])   # one value per CEP
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

  # Compute hourly rain per CEP (grouped by init_utc × cep)
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

# ---- 6d) Export spatial outputs ----
#
# Three complementary formats, each optimised for a different use case:
#
#  "geojson_nested"  ← ONE feature per CEP, hourly values as JSON arrays
#                      ~40x fewer features than long format
#                      Best for: Lovable, web maps, R/leaflet
#                      Typical size: ~5-15 MB for 40K CEPs x 72h
#
#  "fgb"             ← FlatGeobuf: binary, spatially indexed, streamable
#                      Same long-format data but ~60% smaller than GeoJSON
#                      Best for: QGIS (opens instantly), large datasets
#
#  "shp"             ← Shapefile long format, UTF-8 encoded
#                      Best for: ArcGIS, legacy GIS tools
#
# Size-reduction techniques applied in all formats:
#   • lon/lat rounded to 5 decimal places (~1 m precision — more than enough)
#   • rain_mm rounded to 3 decimal places (sub-micron precision is noise)
#   • forecast_hour stored as integer
#   • ISO timestamps stored as compact strings (no redundant timezone suffix)
#
# Size comparison for 40K CEPs × 13 hours (your current run):
#   Long GeoJSON (old)    : ~500 MB
#   Nested GeoJSON (new)  : ~12 MB   (40x smaller)
#   FlatGeobuf            : ~35 MB   long-format but binary
#   Shapefile             : ~120 MB  long-format

export_spatial <- function(df_multi, run_dt,
                           output_dir = "outputs",
                           formats    = c("geojson_nested", "fgb", "shp"),
                           verbose    = TRUE) {

  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  stem    <- sprintf("wrf_cep_forecast_%s", format(run_dt, "%Y%m%d%H"))
  written <- character(0)

  # ------------------------------------------------------------------
  # Base table: one row per CEP × hour, clean types, rounded values
  # ------------------------------------------------------------------
  df_base <- df_multi |>
    dplyr::filter(!is.na(rain_mm)) |>
    dplyr::mutate(
      lon           = round(lon, 5),          # ~1 m precision
      lat           = round(lat, 5),
      rain_mm       = round(rain_mm,   3),    # 0.001 mm resolution
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

  # ==================================================================
  # FORMAT 1 — Nested GeoJSON
  # One feature per CEP; hourly time series encoded as parallel arrays.
  #
  # Each feature's properties look like:
  # {
  #   "cep": "89201100", "municipio": "Joinville", ...
  #   "hours":      [1, 2, 3, ...],
  #   "valid_utc":  ["2026-03-04T01:00:00Z", ...],
  #   "rain_mm":    [0.000, 0.005, 0.016, ...],
  #   "rain_class": ["none","light","light", ...],
  #   "max_rain_mm": 0.025,        ← quick filter without unpacking arrays
  #   "total_rain_mm": 0.041       ← total accumulated over forecast window
  # }
  #
  # In Lovable / Mapbox: colour by max_rain_mm for a static overview,
  # or unpack the arrays client-side for a time-slider animation.
  # ==================================================================
  if ("geojson_nested" %in% formats) {
    if (verbose) cat("\nBuilding nested GeoJSON (1 feature per CEP)...\n")

    if (!requireNamespace("jsonlite", quietly = TRUE))
      stop("Package 'jsonlite' required. Install with: install.packages('jsonlite')")

    # Step 1 — scalar summaries
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

    # Step 2 — array columns
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

    # ------------------------------------------------------------------
    # Write GeoJSON manually with jsonlite so array-valued properties
    # become TRUE JSON arrays  ([0.0, 0.005, ...])  instead of the
    # escaped strings  ("[0.0,0.005,...]")  that sf::st_write produces.
    # This is the key to keeping the file small (~12 MB vs ~85 MB).
    # ------------------------------------------------------------------
    gj_path <- file.path(output_dir, paste0(stem, "_nested.geojson"))

    if (verbose) cat(sprintf("  Building %d features (vectorised)...\n", nrow(cep_meta)))

    # lapply over index — avoids slow row-by-row data frame subsetting
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

  # ==================================================================
  # FORMAT 2 — FlatGeobuf (.fgb)
  # Binary, spatially indexed, streamable over HTTP.
  # Same long-format data as the old GeoJSON but ~60% smaller.
  # Opens natively in QGIS 3.16+ (drag and drop).
  # Can be streamed in Mapbox/Leaflet via the flatgeobuf JS library.
  # ==================================================================
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

  # ==================================================================
  # FORMAT 3 — Shapefile (long format, UTF-8)
  # Column names truncated to 10 chars (shapefile limit).
  # .cpg sidecar ensures QGIS/ArcGIS read accented names correctly.
  # ==================================================================
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
    writeLines("UTF-8", sub("\\.shp$", ".cpg", shp_path))  # encoding sidecar

    sz <- sum(file.info(list.files(shp_dir, full.names = TRUE))$size,
              na.rm = TRUE) / 1024^2
    written <- c(written, shp_path)
    if (verbose)
      cat(sprintf("  -> %s  |  %d features  |  %.1f MB total\n",
                  basename(shp_path), nrow(sf_shp), sz))
  }

  # ------------------------------------------------------------------
  # Summary
  # ------------------------------------------------------------------
  if (verbose) {
    cat(sprintf("\n%d CEPs  x  %d forecast hours  =  %d data points\n",
                n_cep, n_hours, n_cep * n_hours))
    cat("\nRain intensity summary:\n")
    print(table(df_base$rain_class))
    cat(sprintf("\nRain range (mm):  min=%.3f  median=%.3f  max=%.3f  total=%s mm\n",
                min(df_base$rain_mm,    na.rm = TRUE),
                median(df_base$rain_mm, na.rm = TRUE),
                max(df_base$rain_mm,    na.rm = TRUE),
                format(sum(df_base$rain_mm, na.rm = TRUE), big.mark = ",")))
    cat("\nFiles written:\n")
    for (f in written)
      cat(sprintf("  %-55s  %.1f MB\n", f,
                  file.info(f)$size / 1024^2))
  }

  invisible(written)
}

# ---- 6e) Quick R map using ggplot2 (one facet per hour) ----
#
# Requires the SC municipalities shapefile for background.
# Set n_hours_shown to limit the number of facets (e.g. first 12 hours).

plot_rain_map <- function(df_multi, shp_path,
                          n_hours_shown = 12,
                          title         = "WRF hourly rainfall forecast") {

  if (!requireNamespace("viridis", quietly = TRUE))
    stop("Package 'viridis' required. Install with: install.packages('viridis')")

  # SC background
  sc_sf <- sf::st_read(shp_path, quiet = TRUE) |> sf::st_transform(4326)

  # Filter to first n_hours_shown
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
# STEP 6 — MAIN: build CEP list from Excel, extract, export
# =============================================================

# ---- 6A) Load pre-built CEP list ----
#
# Generated once by 00_build_cep_list.R
# Contains one row per geocoded CEP point across all SC municipalities.
# Columns: municipio, localidade, logradouro, cep, cep_fmt, label, lon, lat

if (!file.exists(CEP_LIST_PATH))
  stop(
    "CEP list not found: ", CEP_LIST_PATH,
    "\n  Run 00_build_cep_list.R first to generate it.",
    "\n  It only needs to run once (or when you want finer/coarser resolution)."
  )

cat("\nLoading CEP list from: ", CEP_LIST_PATH, "\n")
cep_df <- readRDS(CEP_LIST_PATH)

# Ensure CEP column is always clean 8-digit string regardless of how it was saved
cep_df$cep <- normalise_cep(cep_df$cep)
cep_sf  <- sf::st_as_sf(cep_df, coords = c("lon","lat"), crs = 4326, remove = FALSE)

cat(sprintf("  %d geocoded CEP points loaded (%d municipalities)\n",
            nrow(cep_sf), length(unique(cep_df$municipio))))
cat(sprintf("  Sample:\n"))
print(head(cep_df[, intersect(c("municipio","localidade","cep_fmt","lon","lat"), names(cep_df))], 5))

# ---- Extract rainfall for all CEPs x all hours ----
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

# ---- Export GeoJSON + Shapefile ----
gc()  # release df_multi memory before building GeoJSON structures
cat("\nExporting spatial files...\n")
export_spatial(
  df_multi   = df_multi,
  run_dt     = run_dt,
  output_dir = "outputs",
  formats    = EXPORT_FORMATS,   # set in Step 0 config; GHA uses geojson_nested only
  verbose    = TRUE
)

# ---- Quick R facet map ----
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
