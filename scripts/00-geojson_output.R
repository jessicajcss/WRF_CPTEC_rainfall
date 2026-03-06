###############################################################
# WRF / GRIB2 – hourly forecast pipeline for Santa Catarina (SC)
#
# Outputs (all are HOURLY; no accumulated-only exports):
#  - Municipality (SC) hourly polygon means (GeoJSON + optional fgb/shp)
#  - CEP hourly points (nested GeoJSON + optional fgb/shp)
#  - Grid hourly points (nested JSON + HTML maps)
#  - PMTiles (vector tiles) for web mapping, kept <100MB in GHA
#
# Notes:
#  - GRIB2 is raster grid data. We crop to SC bbox and write GeoTIFF.
#  - On some Windows installs, terra/GDAL cannot read GRIB2.
#    In that case, run via GitHub Actions (Linux runner) or install a GRIB-enabled GDAL.
#
# Steps
#  0) Config
#  1) Download GRIB2
#  2) Crop & convert -> *_vars.tif (precip_acc_mm, t2m_k, wind_gust_ms)
#  3) Discover files
#  4) Compute TRUE hourly rain (diff of accumulation within init_utc)
#  5) (Optional) single-location diagnostic plot
#  6) CEP export (all SC CEPs)
#  6b) Municipality export (all SC municipalities)
#  7) Lovable artifacts (fast path)
#  8) Grid export + HTML maps + PMTiles
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
# GHA MODE
# =============================================================
GHA_MODE <- identical(Sys.getenv("GHA_MODE"), "true")

if (GHA_MODE) {
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

utc_to_brt    <- function(x) lubridate::with_tz(x, tzone = BRT_TZ)
fmt_brt_label <- function(x) paste0(format(utc_to_brt(x), "%d/%m %H:%M"), " BRT")
fmt_utc_iso   <- function(x) format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ")
fmt_brt_iso   <- function(x) format(as.POSIXct(x, tz = "UTC"), tz = BRT_TZ, "%Y-%m-%dT%H:%M:%S%z")

# =============================================================
# STEP 0 — USER CONFIGURATION (edit this block only)
# =============================================================

# Outputs to generate:
EXPORT_CEPS          <- TRUE
EXPORT_MUNICIPALITY  <- TRUE
EXPORT_GRID          <- TRUE

# Optional single target plot (local dev); in GHA default off to save time
PLOT_SINGLE_TARGET <- !GHA_MODE

# If plotting, choose one:
OUTPUT_MODE <- if (PLOT_SINGLE_TARGET) "municipality" else "all"
MUNICIPIO_NAME <- "Joinville"
CEP            <- "89201-100"

# SC municipal shapefile (used for crop bbox and for municipality summaries)
SHP_PATH <- "./data/shp/SC_Municipios_2024/SC_Municipios_2024.shp"

# Run selection
RUN_START <- NULL
if (GHA_MODE && nzchar(Sys.getenv("WRF_RUN_START")))
  RUN_START <- Sys.getenv("WRF_RUN_START")

# Forecast window (hours)
FORECAST_START_HR <- 0
FORECAST_END_HR   <- 72
if (GHA_MODE && nzchar(Sys.getenv("WRF_FORECAST_END_HR")))
  FORECAST_END_HR <- as.integer(Sys.getenv("WRF_FORECAST_END_HR"))

# Paths/workers
OUTPUT_DIR <- "data/WRF_downloads"
N_WORKERS  <- if (GHA_MODE) 2L else min(8L, max(1L, parallel::detectCores(logical = FALSE)))

# CEP list (pre-built)
CEP_LIST_PATH <- "outputs/cep_list.rds"

# Export formats for vector long outputs
EXPORT_FORMATS <- if (GHA_MODE) c("geojson_nested") else c("geojson_nested", "fgb", "shp")

# Grid exports / html / pmtiles
GRID_EXPORT_JSON <- TRUE
GRID_EXPORT_HTML <- TRUE
GRID_EXPORT_PM_TILES <- TRUE

# Keep committed artifacts small in GHA
MAX_ARTIFACT_MB_GHA <- 95

# Crop behavior
SKIP_DOWNLOAD   <- FALSE
SKIP_CROP       <- FALSE
CROP_BUFFER_DEG <- 0.5

# =============================================================
# Helpers
# =============================================================
`%||%` <- function(a, b) if (!is.null(a)) a else b

normalise_cep <- function(x) {
  formatC(as.integer(gsub("\\D", "", as.character(x))), width = 8L, flag = "0")
}

parse_yyyymmddhh_utc <- function(x) lubridate::ymd_h(x, tz = "UTC")

# =============================================================
# STEP 1 — DOWNLOADER
# =============================================================
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
    manifest_append  = TRUE
) {
  run_dt <- if (is.character(run_start)) lubridate::ymd_hm(run_start, tz = "UTC") else run_start
  if (is.na(run_dt)) stop("Invalid run_start.")

  year  <- lubridate::year(run_dt);   month <- lubridate::month(run_dt)
  day   <- lubridate::day(run_dt);    hour  <- lubridate::hour(run_dt)

  base_url <- sprintf(
    "https://dataserver.cptec.inpe.br/dataserver_modelos/wrf/ams_07km/brutos/%04d/%02d/%02d/%02d/",
    year, month, day, hour)

  dated_dir <- file.path(output_dir, format(run_dt, "%Y%m%d"))
  dir.create(dated_dir, showWarnings = FALSE, recursive = TRUE)

  if (verbose) {
    cat("================================================================\n")
    cat(sprintf("Run init  : %s UTC (%02dZ)\n", format(run_dt, "%Y-%m-%d %H:%M"), hour))
    cat(sprintf("Run init  : %s\n", fmt_brt_label(run_dt)))
    cat(sprintf("Fcst hrs  : %d to %d\n", forecast_start, forecast_end))
    cat(sprintf("Base URL  : %s\n", base_url))
    cat(sprintf("Local dir : %s\n", dated_dir))
    cat("================================================================\n\n")
  }

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
  file_df$forecast_hour <- as.numeric(difftime(file_df$forecast_dt, file_df$run_dt, units="hours"))

  if (!is.null(include_ext)) file_df <- file_df[file_df$extension %in% include_ext, ]
  if (!is.null(exclude_ext)) file_df <- file_df[!file_df$extension %in% exclude_ext, ]
  file_df <- file_df[file_df$forecast_hour >= forecast_start &
                       file_df$forecast_hour <= forecast_end, ]
  if (!nrow(file_df)) { cat("No files match the specified window.\n"); return(invisible(NULL)) }

  file_df$hour_folder <- if (by_hour) sprintf(hour_folder_fmt, as.integer(file_df$forecast_hour)) else ""
  file_df$local_dir   <- if (by_hour) file.path(dated_dir, file_df$hour_folder) else dated_dir
  file_df$local_path  <- file.path(file_df$local_dir, file_df$filename)
  file_df$url         <- paste0(base_url, file_df$filename)

  for (d in unique(file_df$local_dir)) dir.create(d, showWarnings=FALSE, recursive=TRUE)

  # "done" if GRIB2 OR cropped vars tif exists
  tif_paths <- sub("\\.grib2$", "_vars.tif", file_df$local_path, ignore.case=TRUE)
  file_df$exists <- file.exists(file_df$local_path) | file.exists(tif_paths)
  to_get <- file_df[!file_df$exists, ]

  if (!nrow(to_get)) {
    cat("All requested files already exist on disk. Skipping download.\n")
    return(invisible(list(run_dir = dated_dir)))
  }

  if (!requireNamespace("progressr", quietly=TRUE))
    stop("Package 'progressr' required.")
  library(progressr)
  if (requireNamespace("cli", quietly=TRUE)) progressr::handlers("cli") else progressr::handlers("txtprogressbar")

  future::plan(future::multisession, workers=n_workers)

  dl_one <- function(p, url, destfile, filename, fhour, local_dir, valid_dt) {
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
      return(TRUE)
    }
    p(sprintf("FAILED h%03d %s", as.integer(fhour), filename))
    FALSE
  }

  progressr::with_progress({
    p <- progressr::progressor(steps=nrow(to_get))
    ok <- furrr::future_pmap_lgl(
      list(p=replicate(nrow(to_get), p, simplify=FALSE),
           url=to_get$url, destfile=to_get$local_path,
           filename=to_get$filename, fhour=to_get$forecast_hour,
           local_dir=to_get$local_dir, valid_dt=to_get$forecast_dt),
      dl_one, .options=furrr::furrr_options(seed=TRUE))
    if (!all(ok)) warning("Some downloads failed.")
  })

  future::plan(future::sequential)

  if (manifest) {
    dir.create(manifest_dir, showWarnings=FALSE, recursive=TRUE)
    if (is.null(manifest_name))
      manifest_name <- sprintf("manifest_WRF_%s_h%d-%d", format(run_dt,"%Y%m%d%H"), forecast_start, forecast_end)
    write_manifest_csv(file_df, file.path(manifest_dir, paste0(manifest_name,".csv")), append=manifest_append)
  }

  invisible(list(run_dir = dated_dir))
}

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
# STEP 2 — CROP & DELETE (write *_vars.tif)
# =============================================================

pick_precip_layer <- function(r, verbose=FALSE) {
  nm <- names(r)
  try_idx <- function(pat) grep(pat, nm, ignore.case=TRUE)

  idx <- try_idx("Total precipitation")
  if (length(idx)) return(r[[idx[1]]])

  ic <- try_idx("Convective precipitation")
  il <- try_idx("Large.scale precipitation|Large scale precipitation")
  if (length(ic) && length(il)) return(r[[ic[1]]] + r[[il[1]]])
  if (length(ic)) return(r[[ic[1]]])
  if (length(il)) return(r[[il[1]]])

  idx <- try_idx("APCP")
  if (length(idx)) return(r[[idx[1]]])

  idx <- try_idx("precip|rain|prate|tp")
  if (length(idx)) return(r[[idx[1]]])

  stop("No precipitation layer found.\nLayer names:\n", paste(nm, collapse="\n"))
}

pick_temp2m_layer <- function(r) {
  nm <- names(r)
  idx <- grep("2 metre temperature|2m temperature|TMP.*2 m|T2M|t2m", nm, ignore.case=TRUE)
  if (length(idx)) return(r[[idx[1]]])
  idx <- grep("temperature|TMP", nm, ignore.case=TRUE)
  if (length(idx)) return(r[[idx[1]]])
  NULL
}

pick_wind_gust_layer <- function(r) {
  nm <- names(r)
  idx <- grep("gust|GUST", nm, ignore.case=TRUE)
  if (length(idx)) return(r[[idx[1]]])
  idx <- grep("10 metre.*wind|10m wind|wind speed|WIND10|WSPD10", nm, ignore.case=TRUE)
  if (length(idx)) return(r[[idx[1]]])
  NULL
}

get_crop_extent <- function(shp_path, buffer_deg=0.5) {
  v   <- terra::vect(shp_path)
  ext <- terra::ext(v)
  terra::ext(ext$xmin - buffer_deg, ext$xmax + buffer_deg,
             ext$ymin - buffer_deg, ext$ymax + buffer_deg)
}

# Detect GRIB read failure early (Windows often fails)
can_read_grib2 <- function(grib2_path) {
  isTRUE(tryCatch({
    r <- terra::rast(grib2_path)
    terra::nlyr(r) >= 1
  }, error = function(e) FALSE))
}

crop_and_save_one_vars <- function(grib2_path, crop_ext_lonlat) {
  tif_path <- sub("\\.grib2$", "_vars.tif", grib2_path, ignore.case=TRUE)

  if (file.exists(tif_path) && !file.exists(grib2_path)) return(tif_path)
  if (file.exists(tif_path) && file.exists(grib2_path)) { file.remove(grib2_path); return(tif_path) }

  r <- terra::rast(grib2_path)

  p <- pick_precip_layer(r)
  t <- pick_temp2m_layer(r)
  w <- pick_wind_gust_layer(r)

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
      t_crop <- terra::resample(t_crop, p_crop, method="bilinear")
  } else { t_crop <- p_crop; terra::values(t_crop) <- NA_real_ }

  if (!is.null(w)) {
    w_crop <- terra::crop(w, crop_ext_use)
    if (!isTRUE(all.equal(terra::res(w_crop), terra::res(p_crop))) ||
        !isTRUE(all.equal(terra::ext(w_crop), terra::ext(p_crop))))
      w_crop <- terra::resample(w_crop, p_crop, method="bilinear")
  } else { w_crop <- p_crop; terra::values(w_crop) <- NA_real_ }

  vars <- c(p_crop, t_crop, w_crop)
  names(vars) <- c("precip_acc_mm", "t2m_k", "wind_gust_ms")

  terra::writeRaster(vars, tif_path, overwrite=TRUE, gdal=c("COMPRESS=LZW"))
  file.remove(grib2_path)
  tif_path
}

crop_run_directory_vars <- function(run_dir, shp_path, buffer_deg=0.5, verbose=TRUE) {
  grib2_files <- list.files(run_dir, pattern="\\.grib2$", full.names=TRUE, recursive=TRUE)
  if (!length(grib2_files)) {
    message("No .grib2 files to crop in: ", run_dir)
    return(invisible(NULL))
  }

  if (!can_read_grib2(grib2_files[1])) {
    stop(
      "This environment cannot read GRIB2 with terra/GDAL (failed on: ", basename(grib2_files[1]), ").\n",
      "Run this pipeline in GitHub Actions (Linux), or install a GRIB-enabled GDAL/terra build.\n"
    )
  }

  crop_ext <- get_crop_extent(shp_path, buffer_deg)

  if (verbose) cat(sprintf("\nCropping %d GRIB2 → multi-var GeoTIFF (_vars.tif)\n", length(grib2_files)))

  res <- vapply(seq_along(grib2_files), function(i) {
    f <- grib2_files[i]
    if (verbose) message(sprintf("  [%d/%d] %s", i, length(grib2_files), basename(f)))
    tryCatch(crop_and_save_one_vars(f, crop_ext), error = function(e) {
      warning("Crop failed: ", basename(f), " :: ", conditionMessage(e))
      NA_character_
    })
  }, character(1))

  invisible(res)
}

# =============================================================
# STEP 3 — FILE DISCOVERY (prefer *_vars.tif)
# =============================================================
discover_wrf_files <- function(run_dir) {
  files <- list.files(run_dir, pattern="_vars\\.tif$", full.names=TRUE, recursive=TRUE)
  ext_used <- "vars.tif"
  if (!length(files)) {
    files <- list.files(run_dir, pattern="\\.tif$", full.names=TRUE, recursive=TRUE)
    ext_used <- "tif"
  }
  if (!length(files)) {
    files <- list.files(run_dir, pattern="\\.grib2$", full.names=TRUE, recursive=TRUE)
    ext_used <- "grib2"
  }
  if (!length(files)) stop("No files found under: ", run_dir)
  message(sprintf("Discovered %d %s files.", length(files), ext_used))

  base <- tools::file_path_sans_ext(basename(files))
  base <- sub("_vars$", "", base)

  p1 <- "^WRF_cpt_07KM_(\\d{10})_(\\d{10})$"
  p2 <- "^WRF_cpt_07KM_(\\d{10})$"
  is_p1 <- grepl(p1, base)
  is_p2 <- grepl(p2, base) & !is_p1

  init_str  <- dplyr::case_when(is_p1~sub(p1,"\\1",base), is_p2~sub(p2,"\\1",base), TRUE~NA_character_)
  valid_str <- dplyr::case_when(is_p1~sub(p1,"\\2",base), is_p2~sub(p2,"\\1",base), TRUE~NA_character_)

  init_utc  <- lubridate::ymd_h(init_str,  tz="UTC")
  valid_utc <- lubridate::ymd_h(valid_str, tz="UTC")

  tibble::tibble(
    file          = files,
    init_utc      = init_utc,
    valid_utc     = valid_utc,
    forecast_hour = as.numeric(difftime(valid_utc, init_utc, units="hours")),
    valid_brt_lbl = vapply(valid_utc, fmt_brt_label, character(1))
  ) |> dplyr::arrange(init_utc, forecast_hour)
}

# =============================================================
# STEP 4 — TRUE hourly rain (diff accumulation)
# =============================================================
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

extract_precip_acc_layer <- function(r) {
  if (terra::nlyr(r) == 1) return(r)
  if ("precip_acc_mm" %in% names(r)) return(r[["precip_acc_mm"]])
  pick_precip_layer(r)
}

extract_one_file_mean <- function(file, target_vect) {
  r <- terra::rast(file)
  p <- extract_precip_acc_layer(r)

  if (!terra::same.crs(target_vect, p))
    target_vect <- terra::project(target_vect, terra::crs(p))

  v <- terra::extract(p, target_vect, fun=mean, na.rm=TRUE)
  as.numeric(v[1, 2])
}

# =============================================================
# STEP 5 — (Optional) Single target plot
# =============================================================
plot_rain_bar <- function(df, title=NULL, subtitle=NULL) {
  df_plot <- dplyr::filter(df, !is.na(rain_mm))
  if (!nrow(df_plot)) return(invisible(NULL))
  p <- ggplot2::ggplot(df_plot, ggplot2::aes(x=valid_utc, y=rain_mm)) +
    ggplot2::geom_col(fill="#2166ac", width=3600 * 0.9) +
    ggplot2::scale_x_datetime(date_labels="%d/%m\n%H:%M", date_breaks="6 hours") +
    ggplot2::labs(title=title, subtitle=subtitle,
                  x="Valid time (UTC)", y="Rain (mm/h)") +
    ggplot2::theme_minimal(base_size=13)
  if (!GHA_MODE) print(p)
  invisible(p)
}

# =============================================================
# STEP 6 — CEP export (all SC CEPs)
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

extract_rain_multi_cep <- function(run_dir, cep_sf, verbose = TRUE) {
  meta <- discover_wrf_files(run_dir)
  pts_vect <- terra::vect(cep_sf)

  all_out <- vector("list", nrow(meta))

  for (i in seq_len(nrow(meta))) {
    fh  <- meta$forecast_hour[i]
    if (verbose) message(sprintf("  [CEP %d/%d] h%03d  %s (%s)",
                                 i, nrow(meta), fh, basename(meta$file[i]), meta$valid_brt_lbl[i]))

    f <- meta$file[i]
    vals <- tryCatch({
      r <- terra::rast(f)
      p <- extract_precip_acc_layer(r)
      pts2 <- if (!terra::same.crs(pts_vect, p)) terra::project(pts_vect, terra::crs(p)) else pts_vect
      v <- terra::extract(p, pts2, fun = mean, na.rm = TRUE)
      as.numeric(v[, 2])
    }, error = function(e) {
      warning(sprintf("h%03d extract failed: %s", fh, conditionMessage(e)))
      rep(NA_real_, nrow(cep_sf))
    })

    all_out[[i]] <- data.frame(
      init_utc      = meta$init_utc[i],
      forecast_hour = fh,
      valid_utc     = meta$valid_utc[i],
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

export_ceps_nested_geojson <- function(df_multi, run_dt, output_dir = "outputs", verbose = TRUE) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Package 'jsonlite' required.")
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

  stem <- sprintf("wrf_cep_forecast_%s", format(run_dt, "%Y%m%d%H"))

  df_base <- df_multi |>
    dplyr::filter(!is.na(rain_mm)) |>
    dplyr::mutate(
      lon           = round(lon, 5),
      lat           = round(lat, 5),
      rain_mm       = round(rain_mm,   3),
      accum_mm      = round(accum_mm,  3),
      forecast_hour = as.integer(forecast_hour),
      valid_utc     = format(valid_utc, "%Y-%m-%dT%H:%M:%SZ"),
      init_utc      = format(init_utc,  "%Y-%m-%dT%H:%M:%SZ")
    ) |>
    dplyr::select(municipio, cep, label, lon, lat, init_utc, valid_utc, forecast_hour, accum_mm, rain_mm, rain_class)

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

  features <- lapply(seq_len(nrow(cep_meta)), function(i) {
    list(
      type     = "Feature",
      geometry = list(type = "Point", coordinates = c(cep_meta$lon[i], cep_meta$lat[i])),
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
    type = "FeatureCollection",
    crs  = list(type="name", properties=list(name="urn:ogc:def:crs:OGC:1.3:CRS84")),
    features = features
  )

  gj_path <- file.path(output_dir, paste0(stem, "_nested.geojson"))
  jsonlite::write_json(geojson_obj, gj_path, auto_unbox = TRUE, digits = 6, pretty = FALSE)

  if (verbose) cat(sprintf("CEP nested GeoJSON: %s (%.1f MB)\n", gj_path, file.info(gj_path)$size/1024^2))
  invisible(gj_path)
}

# =============================================================
# STEP 6b — Municipality export (ALL SC municipalities)
# =============================================================
extract_rain_all_municipalities <- function(run_dir, shp_path, verbose = TRUE) {
  meta <- discover_wrf_files(run_dir)
  mun <- terra::vect(shp_path)

  # Try to find name column
  cands <- c("NM_MUN","NM_MUNICIP","NOME","NAME","MUNICIPIO")
  name_col <- intersect(names(mun), cands)[1]
  if (is.na(name_col)) stop("Cannot find municipality name column in shapefile. Columns: ", paste(names(mun), collapse=", "))

  mun_names <- as.character(mun[[name_col]])
  mun$mun_name <- mun_names

  out <- vector("list", nrow(meta))
  for (i in seq_len(nrow(meta))) {
    fh <- meta$forecast_hour[i]
    if (verbose) message(sprintf("  [MUN %d/%d] h%03d  %s (%s)",
                                 i, nrow(meta), fh, basename(meta$file[i]), meta$valid_brt_lbl[i]))
    f <- meta$file[i]
    vals <- tryCatch({
      r <- terra::rast(f)
      p <- extract_precip_acc_layer(r)
      mun2 <- if (!terra::same.crs(mun, p)) terra::project(mun, terra::crs(p)) else mun
      v <- terra::extract(p, mun2, fun = mean, na.rm = TRUE)
      as.numeric(v[, 2])
    }, error = function(e) {
      warning("Municipality extract failed: ", conditionMessage(e))
      rep(NA_real_, length(mun))
    })

    out[[i]] <- data.frame(
      init_utc      = meta$init_utc[i],
      forecast_hour = as.integer(fh),
      valid_utc     = meta$valid_utc[i],
      mun_name      = mun$mun_name,
      accum_mm      = vals,
      stringsAsFactors = FALSE
    )
  }

  df <- dplyr::bind_rows(out)
  df <- df |>
    dplyr::arrange(init_utc, mun_name, forecast_hour) |>
    dplyr::group_by(init_utc, mun_name) |>
    dplyr::mutate(
      rain_mm = dplyr::if_else(dplyr::row_number() == 1L, NA_real_, pmax(0, accum_mm - dplyr::lag(accum_mm))),
      rain_class = classify_rain(rain_mm)
    ) |>
    dplyr::ungroup()

  df
}

export_municipalities_hourly_geojson <- function(df_mun, shp_path, run_dt, out_dir = "outputs", verbose = TRUE) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Package 'jsonlite' required.")
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  stem <- sprintf("wrf_municipalities_sc_%s_hourly", format(run_dt, "%Y%m%d%H"))

  # Join municipality geometry by name
  sc_sf <- sf::st_read(shp_path, quiet = TRUE) |> sf::st_transform(4326)
  cands <- c("NM_MUN","NM_MUNICIP","NOME","NAME","MUNICIPIO")
  name_col <- intersect(names(sc_sf), cands)[1]
  if (is.na(name_col)) stop("Cannot find municipality name column in shapefile.")

  sc_sf$mun_name <- as.character(sc_sf[[name_col]])

  df_out <- df_mun |>
    dplyr::filter(!is.na(rain_mm)) |>
    dplyr::mutate(
      init_utc   = fmt_utc_iso(init_utc),
      valid_utc  = fmt_utc_iso(valid_utc),
      rain_mm    = round(rain_mm, 3),
      accum_mm   = round(accum_mm, 3)
    )

  # Long GeoJSON (one feature per muni-hour) can be big.
  # In GHA we keep it small by exporting ONLY a "nested per municipality" format.
  if (GHA_MODE) {
    nested_path <- file.path(out_dir, paste0(stem, "_nested.geojson"))

    scalars <- df_out |>
      dplyr::group_by(mun_name) |>
      dplyr::summarise(
        init_utc = dplyr::first(init_utc),
        max_rain_mm = max(rain_mm, na.rm = TRUE),
        total_rain_mm = round(sum(rain_mm, na.rm = TRUE), 3),
        .groups = "drop"
      )

    arrays <- df_out |>
      dplyr::arrange(mun_name, forecast_hour) |>
      dplyr::group_by(mun_name) |>
      dplyr::summarise(
        hours = list(as.integer(forecast_hour)),
        valid_utc = list(valid_utc),
        rain_mm = list(rain_mm),
        rain_class = list(rain_class),
        .groups = "drop"
      )

    meta <- dplyr::left_join(scalars, arrays, by="mun_name")
    meta <- dplyr::left_join(meta, dplyr::select(sc_sf, mun_name, geometry), by="mun_name")

    feats <- lapply(seq_len(nrow(meta)), function(i) {
      g <- sf::st_as_geojson(meta$geometry[i], digits = 6)
      list(
        type = "Feature",
        geometry = jsonlite::fromJSON(g, simplifyVector = FALSE),
        properties = list(
          mun_name = meta$mun_name[i],
          init_utc = meta$init_utc[i],
          max_rain_mm = meta$max_rain_mm[i],
          total_rain_mm = meta$total_rain_mm[i],
          hours = meta$hours[[i]],
          valid_utc = meta$valid_utc[[i]],
          rain_mm = meta$rain_mm[[i]],
          rain_class = meta$rain_class[[i]]
        )
      )
    })

    obj <- list(type="FeatureCollection", features=feats)
    jsonlite::write_json(obj, nested_path, auto_unbox = TRUE, pretty = FALSE)

    if (verbose) cat(sprintf("Municipalities nested GeoJSON: %s (%.1f MB)\n",
                             nested_path, file.info(nested_path)$size/1024^2))
    return(invisible(nested_path))
  }

  # Local: also write long GeoJSON for GIS if desired
  long_path <- file.path(out_dir, paste0(stem, ".geojson"))
  sf_long <- dplyr::left_join(df_out, dplyr::select(sc_sf, mun_name, geometry), by="mun_name") |>
    sf::st_as_sf()

  sf::st_write(sf_long, long_path, delete_dsn = TRUE, quiet = TRUE)
  if (verbose) cat(sprintf("Municipalities long GeoJSON: %s (%.1f MB)\n", long_path, file.info(long_path)$size/1024^2))
  invisible(long_path)
}

# =============================================================
# STEP 7 — Lovable artifacts (from CEP nested geojson)
# =============================================================
compute_heat_weight <- function(rain_mm) {
  pmin(sqrt(pmax(rain_mm, 0)) / sqrt(50), 1)
}

build_points_lite_from_nested <- function(nested_geojson_path, mode = c("now", "peak"), windowHours = 24L) {
  mode <- match.arg(mode)
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Package 'jsonlite' required.")

  obj <- jsonlite::fromJSON(nested_geojson_path, simplifyVector = FALSE)
  feats <- obj$features %||% list()

  nowUtc <- as.POSIXct(Sys.time(), tz = "UTC")
  windowEnd <- nowUtc + as.numeric(windowHours) * 3600

  out_feats <- lapply(feats, function(f) {
    props <- f$properties %||% list()
    valid <- props$valid_utc
    rain  <- props$rain_mm

    rainNow <- 0; nowTsBrt <- ""
    peakMm <- 0; peakTsBrt <- ""

    if (!is.null(valid) && !is.null(rain) && length(valid) > 0 && length(rain) > 0) {
      ts <- as.POSIXct(unlist(valid), tz = "UTC")
      rr <- suppressWarnings(as.numeric(unlist(rain)))
      ok <- is.finite(as.numeric(ts)) & is.finite(rr)
      ts <- ts[ok]; rr <- rr[ok]

      if (length(ts) > 0) {
        diffs <- abs(as.numeric(difftime(ts, nowUtc, units = "secs")))
        i_now <- which.min(diffs)
        rainNow <- rr[i_now]
        nowTsBrt <- paste0(format(ts[i_now], tz = BRT_TZ, "%d/%m %H:%M"), " BRT")

        in_win <- ts <= windowEnd
        if (any(in_win)) {
          rr_win <- rr[in_win]; ts_win <- ts[in_win]
          i_peak <- which.max(rr_win)
          peakMm <- rr_win[i_peak]
          peakTsBrt <- paste0(format(ts_win[i_peak], tz = BRT_TZ, "%d/%m %H:%M"), " BRT")
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
        cep = props$cep %||% "",
        municipio = props$municipio %||% "",
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

write_points_lite_files <- function(nested_geojson_path, run_dt, out_dir = "outputs", windows = c(12L,24L,48L,72L)) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Package 'jsonlite' required.")
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  run_id <- format(run_dt, "%Y%m%d%H")

  now_obj <- build_points_lite_from_nested(nested_geojson_path, mode="now", windowHours=24L)
  now_obj$`_meta` <- list(run_id = run_id, kind="now", generated_utc = fmt_utc_iso(lubridate::now(tzone="UTC")))

  now_run_path    <- file.path(out_dir, sprintf("points_lite_now_%s.geojson", run_id))
  now_latest_path <- file.path(out_dir, "points_lite_now_latest.geojson")
  jsonlite::write_json(now_obj, now_run_path, auto_unbox=TRUE, pretty=FALSE)
  file.copy(now_run_path, now_latest_path, overwrite=TRUE)

  for (w in windows) {
    peak_obj <- build_points_lite_from_nested(nested_geojson_path, mode="peak", windowHours=as.integer(w))
    peak_obj$`_meta` <- list(run_id = run_id, kind=sprintf("peak_%d", w), generated_utc = fmt_utc_iso(lubridate::now(tzone="UTC")))

    peak_run_path    <- file.path(out_dir, sprintf("points_lite_peak_%d_%s.geojson", w, run_id))
    peak_latest_path <- file.path(out_dir, sprintf("points_lite_peak_%d_latest.geojson", w))
    jsonlite::write_json(peak_obj, peak_run_path, auto_unbox=TRUE, pretty=FALSE)
    file.copy(peak_run_path, peak_latest_path, overwrite=TRUE)
  }

  invisible(TRUE)
}

write_available_runs <- function(run_dt, out_path = "outputs/available_runs.json") {
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Package 'jsonlite' required.")
  run_id <- format(run_dt, "%Y%m%d%H")
  available <- list(
    runs = list(list(
      run_id = run_id,
      run_time_utc = fmt_utc_iso(run_dt),
      label_brt = fmt_brt_label(run_dt)
    )),
    default_run_id = run_id
  )
  jsonlite::write_json(available, out_path, auto_unbox=TRUE, pretty=TRUE)
  invisible(TRUE)
}

write_latest_json <- function(run_dt, out_path = "outputs/latest.json") {
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Package 'jsonlite' required.")
  run_id <- format(run_dt, "%Y%m%d%H")
  latest <- list(
    run_id = run_id,
    cycle  = run_id,
    generated_at = fmt_utc_iso(lubridate::now(tzone="UTC")),
    geojson_url = sprintf("outputs/wrf_cep_forecast_%s_nested.geojson", run_id),
    points_now_url = sprintf("outputs/points_lite_now_%s.geojson", run_id)
  )
  jsonlite::write_json(latest, out_path, auto_unbox=TRUE, pretty=TRUE)
  invisible(TRUE)
}

# =============================================================
# STEP 8 — GRID export (hourly) + HTML + PMTiles
# =============================================================
require_vars_tifs <- function(run_dir) {
  vars <- list.files(run_dir, pattern = "_vars\\.tif$", full.names = TRUE, recursive = TRUE)
  if (!length(vars)) {
    stop(
      "Step 8 requires cropped *_vars.tif files, but none were found under: ", run_dir, "\n",
      "This usually means terra/GDAL cannot read GRIB2 on this machine.\n",
      "Run in GitHub Actions (Linux) or install GRIB-enabled GDAL.\n"
    )
  }
  invisible(vars)
}

read_grid_stack_vars <- function(run_dir, verbose = TRUE) {
  require_vars_tifs(run_dir)

  meta <- discover_wrf_files(run_dir)
  r0 <- terra::rast(meta$file[1])

  need <- c("precip_acc_mm","t2m_k","wind_gust_ms")
  if (!all(need %in% names(r0))) {
    stop("Expected layers not found in *_vars.tif. Need: ", paste(need, collapse=", "),
         "\nGot: ", paste(names(r0), collapse=", "))
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

    p <- terra::resample(p, ref, method="bilinear")
    t <- terra::resample(t, ref, method="bilinear")
    w <- terra::resample(w, ref, method="bilinear")

    acc[, i] <- as.numeric(terra::values(p))
    t2k[, i] <- as.numeric(terra::values(t))
    wnd[, i] <- as.numeric(terra::values(w))
  }

  list(coords = coords, meta = meta, rain_acc = acc, t2m_k = t2k, wind_ms = wnd)
}

grid_hourly_rain <- function(meta, acc) {
  out <- acc * NA_real_
  for (init in unique(meta$init_utc)) {
    idx <- which(meta$init_utc == init)
    idx <- idx[order(meta$forecast_hour[idx])]
    if (length(idx) < 2) next
    for (j in 2:length(idx)) out[, idx[j]] <- pmax(0, acc[, idx[j]] - acc[, idx[j-1]])
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

export_grid_nested_json <- function(st, run_dt, out_path, max_mb = 95, verbose = TRUE) {
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
      max_rain_mm = round(max(rr[ok], na.rm=TRUE), 3),
      total_rain_mm = round(sum(rr[ok], na.rm=TRUE), 3),
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
      tz_display = BRT_TZ,
      n_cells = length(sel),
      n_steps = length(hours_vec)
    ),
    cells = cells
  )

  jsonlite::write_json(obj, out_path, auto_unbox=TRUE, digits=6, pretty=FALSE)
  sz <- file.info(out_path)$size / 1024^2

  if (verbose) cat(sprintf("Grid nested JSON: %s (%.1f MB)\n", out_path, sz))

  # size guard in GHA: drop dry cells
  if (GHA_MODE && sz > max_mb) {
    warning(sprintf("Grid JSON %.1f MB > %d MB; dropping dry cells.", sz, max_mb))
    obj$cells <- Filter(function(x) isTRUE(x$max_rain_mm > 0), obj$cells)
    obj$meta$n_cells <- length(obj$cells)
    obj$meta$dry_cells_removed <- TRUE
    jsonlite::write_json(obj, out_path, auto_unbox=TRUE, digits=6, pretty=FALSE)
    sz2 <- file.info(out_path)$size / 1024^2
    if (verbose) cat(sprintf("Grid nested JSON rewritten: %.1f MB\n", sz2))
  }

  invisible(out_path)
}

build_leaflet_html <- function(df_grid, run_dt, out_path, mode = c("tabs","rain","temp","wind"), verbose=TRUE) {
  mode <- match.arg(mode)
  if (!requireNamespace("jsonlite", quietly=TRUE)) stop("Package 'jsonlite' required.")
  if (!requireNamespace("glue", quietly=TRUE)) stop("Package 'glue' required.")

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

  title_suffix <- switch(mode, tabs="rain/temp/wind", rain="rain", temp="temp", wind="wind")

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
        .bindTooltip(`${{v.toFixed(2)}} mm/h<br>${{d.label_brt}}`, {{sticky:true}})
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
        .bindTooltip(`${{v.toFixed(1)}} °C<br>${{d.label_brt}}`, {{sticky:true}}));
    }}
    layerPts = L.layerGroup(pts).addTo(map);
  }} else {{
    const pts = [];
    for (let i=0; i<d.lat.length; i++) {{
      const v = d.wind_ms[i];
      if (v === null || isNaN(v)) continue;
      pts.push(L.circleMarker([d.lat[i], d.lon[i]], {{radius:5, color:"transparent", fillColor:windColor(v), fillOpacity:0.75, weight:0}})
        .bindTooltip(`${{v.toFixed(1)}} m/s<br>${{d.label_brt}}`, {{sticky:true}}));
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
  if (verbose) cat(sprintf("HTML: %s (%.1f MB)\n", out_path, file.info(out_path)$size/1024^2))
  invisible(out_path)
}

# PMTiles: use tippecanoe + pmtiles CLI (installed in workflow)
build_pmtiles_from_geojson <- function(geojson_path, out_pmtiles, layer_name = "wrf", max_mb = 95) {
  if (!file.exists(geojson_path)) stop("GeoJSON not found: ", geojson_path)

  tmp_mbtiles <- sub("\\.pmtiles$", ".mbtiles", out_pmtiles)
  unlink(tmp_mbtiles)

  # tippecanoe → mbtiles
  cmd1 <- sprintf(
    "tippecanoe -o %s -l %s -zg --drop-densest-as-needed --force %s",
    shQuote(tmp_mbtiles), shQuote(layer_name), shQuote(geojson_path)
  )
  message("Running: ", cmd1)
  st1 <- system(cmd1)
  if (!identical(st1, 0L)) stop("tippecanoe failed for: ", geojson_path)

  # mbtiles → pmtiles
  cmd2 <- sprintf("pmtiles convert %s %s", shQuote(tmp_mbtiles), shQuote(out_pmtiles))
  message("Running: ", cmd2)
  st2 <- system(cmd2)
  if (!identical(st2, 0L)) stop("pmtiles convert failed.")

  sz <- file.info(out_pmtiles)$size / 1024^2
  message(sprintf("PMTiles written: %s (%.1f MB)", out_pmtiles, sz))

  # hard guard in GHA
  if (GHA_MODE && sz > max_mb) {
    stop(sprintf("PMTiles too large (%.1f MB > %d MB): %s", sz, max_mb, out_pmtiles))
  }

  invisible(out_pmtiles)
}

# =============================================================
# MAIN RUN
# =============================================================
run_dt <- if (is.null(RUN_START)) detect_latest_run() else lubridate::ymd_hm(RUN_START, tz="UTC")
run_dir <- file.path(OUTPUT_DIR, format(run_dt, "%Y%m%d"))
dir.create("outputs", showWarnings = FALSE, recursive = TRUE)

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
  message("SKIP_DOWNLOAD=TRUE — using files in: ", run_dir)
}

gc()

if (!SKIP_CROP) {
  crop_run_directory_vars(run_dir, SHP_PATH, buffer_deg = CROP_BUFFER_DEG, verbose = TRUE)
} else {
  message("SKIP_CROP=TRUE — expecting *_vars.tif already present.")
}

# Optional single target plot
if (PLOT_SINGLE_TARGET && OUTPUT_MODE %in% c("municipality","cep")) {
  meta <- discover_wrf_files(run_dir)
  if (OUTPUT_MODE == "municipality") {
    v <- terra::vect(SHP_PATH)
    cands <- c("NM_MUN","NM_MUNICIP","NOME","NAME","MUNICIPIO")
    name_col <- intersect(names(v), cands)[1]
    if (is.na(name_col)) stop("Cannot auto-detect municipality name column.")
    vals <- as.character(v[[name_col]])
    idx  <- which(tolower(vals) == tolower(MUNICIPIO_NAME))
    if (!length(idx)) idx <- grep(tolower(MUNICIPIO_NAME), tolower(vals))
    if (!length(idx)) stop("Municipality not found: ", MUNICIPIO_NAME)
    poly <- v[idx[1]]

    raw <- dplyr::bind_rows(lapply(seq_len(nrow(meta)), function(i) {
      tibble::tibble(
        init_utc = meta$init_utc[i],
        forecast_hour = meta$forecast_hour[i],
        valid_utc = meta$valid_utc[i],
        accum_mm = extract_one_file_mean(meta$file[i], poly)
      )
    }))
    df <- accum_to_hourly(raw)
    plot_rain_bar(df, title=paste0(MUNICIPIO_NAME, " — hourly rain"), subtitle=fmt_brt_label(run_dt))
  } else {
    # CEP: use cep_list if possible
    cep_df0 <- if (file.exists(CEP_LIST_PATH)) readRDS(CEP_LIST_PATH) else NULL
    cep_clean <- normalise_cep(CEP)
    hit <- if (!is.null(cep_df0)) cep_df0[normalise_cep(cep_df0$cep) == cep_clean, ] else NULL
    if (!is.null(hit) && nrow(hit)) {
      pt <- terra::vect(data.frame(lon=hit$lon[1], lat=hit$lat[1]), geom=c("lon","lat"), crs="EPSG:4326")
    } else {
      geo <- geocodebr::busca_por_cep(cep_clean)
      pt <- terra::vect(data.frame(lon=geo$lon[1], lat=geo$lat[1]), geom=c("lon","lat"), crs="EPSG:4326")
    }

    raw <- dplyr::bind_rows(lapply(seq_len(nrow(meta)), function(i) {
      tibble::tibble(
        init_utc = meta$init_utc[i],
        forecast_hour = meta$forecast_hour[i],
        valid_utc = meta$valid_utc[i],
        accum_mm = extract_one_file_mean(meta$file[i], pt)
      )
    }))
    df <- accum_to_hourly(raw)
    plot_rain_bar(df, title=paste0("CEP ", CEP, " — hourly rain"), subtitle=fmt_brt_label(run_dt))
  }
}

# --- Step 6 CEP export ---
nested_cep_path <- NULL
if (EXPORT_CEPS) {
  if (!file.exists(CEP_LIST_PATH)) stop("CEP list not found: ", CEP_LIST_PATH)

  cep_df <- readRDS(CEP_LIST_PATH)
  cep_df$cep <- normalise_cep(cep_df$cep)
  if (!("label" %in% names(cep_df))) cep_df$label <- cep_df$cep
  cep_sf <- sf::st_as_sf(cep_df, coords=c("lon","lat"), crs=4326, remove=FALSE)

  cat(sprintf("\nCEP points loaded: %d\n", nrow(cep_sf)))
  df_ceps <- extract_rain_multi_cep(run_dir, cep_sf, verbose = TRUE)

  # hourly-only (drop accum-only first hour)
  df_ceps <- dplyr::filter(df_ceps, !is.na(rain_mm))

  nested_cep_path <- export_ceps_nested_geojson(df_ceps, run_dt, output_dir="outputs", verbose=TRUE)

  # Size guard (GHA)
  if (GHA_MODE && file.exists(nested_cep_path)) {
    sz <- file.info(nested_cep_path)$size / 1024^2
    if (sz > MAX_ARTIFACT_MB_GHA) stop("CEP nested GeoJSON too large: ", sz, " MB")
  }

  # Lovable artifacts
  if (GHA_MODE) {
    write_points_lite_files(nested_cep_path, run_dt, out_dir="outputs", windows=c(12L,24L,48L,72L))
    write_available_runs(run_dt, out_path="outputs/available_runs.json")
    write_latest_json(run_dt, out_path="outputs/latest.json")
  }
}

# --- Step 6b Municipalities export ---
mun_geojson_path <- NULL
if (EXPORT_MUNICIPALITY) {
  df_mun <- extract_rain_all_municipalities(run_dir, SHP_PATH, verbose=TRUE)
  df_mun <- dplyr::filter(df_mun, !is.na(rain_mm))  # hourly-only
  mun_geojson_path <- export_municipalities_hourly_geojson(df_mun, SHP_PATH, run_dt, out_dir="outputs", verbose=TRUE)

  if (GHA_MODE && file.exists(mun_geojson_path)) {
    sz <- file.info(mun_geojson_path)$size / 1024^2
    if (sz > MAX_ARTIFACT_MB_GHA) stop("Municipalities GeoJSON too large: ", sz, " MB")
  }
}

# --- Step 8 Grid export + HTML + PMTiles ---
if (EXPORT_GRID) {
  st <- read_grid_stack_vars(run_dir, verbose=TRUE)
  df_grid <- grid_to_long_hourly(st)
  df_grid <- dplyr::filter(df_grid, !is.na(rain_mm))  # hourly-only

  run_id <- format(run_dt, "%Y%m%d%H")

  if (GRID_EXPORT_JSON) {
    grid_json <- file.path("outputs", sprintf("wrf_grid_forecast_%s_nested.json", run_id))
    export_grid_nested_json(st, run_dt, grid_json, max_mb = MAX_ARTIFACT_MB_GHA, verbose=TRUE)
  }

  if (GRID_EXPORT_HTML) {
    build_leaflet_html(df_grid, run_dt, file.path("outputs", sprintf("forecast_map_%s.html", run_id)), mode="tabs")
    build_leaflet_html(df_grid, run_dt, file.path("outputs", sprintf("forecast_map_rain_%s.html", run_id)), mode="rain")
    build_leaflet_html(df_grid, run_dt, file.path("outputs", sprintf("forecast_map_temp_%s.html", run_id)), mode="temp")
    build_leaflet_html(df_grid, run_dt, file.path("outputs", sprintf("forecast_map_wind_%s.html", run_id)), mode="wind")
  }

  if (GRID_EXPORT_PM_TILES) {
    # Create PMTiles for CEP points (nested geojson is NOT ideal for tippecanoe),
    # so we create a SMALL long GeoJSON for PMTiles based on the "points_lite_now_latest"
    # and "points_lite_peak_24_latest" which are already compact and hourly-derived.
    #
    # (These are "summary" layers. Hourly full CEP×hour tiles will exceed 100MB.)
    if (GHA_MODE) {
      now_geo <- "outputs/points_lite_now_latest.geojson"
      p24_geo <- "outputs/points_lite_peak_24_latest.geojson"
      if (file.exists(now_geo)) {
        build_pmtiles_from_geojson(now_geo, file.path("outputs", "points_lite_now_latest.pmtiles"), layer_name="now", max_mb=MAX_ARTIFACT_MB_GHA)
      }
      if (file.exists(p24_geo)) {
        build_pmtiles_from_geojson(p24_geo, file.path("outputs", "points_lite_peak_24_latest.pmtiles"), layer_name="peak24", max_mb=MAX_ARTIFACT_MB_GHA)
      }
    }
  }
}

# Final GHA size checks for committed artifacts
if (GHA_MODE) {
  must_exist <- c(
    "outputs/latest.json",
    "outputs/available_runs.json",
    "outputs/points_lite_now_latest.geojson",
    "outputs/points_lite_peak_12_latest.geojson",
    "outputs/points_lite_peak_24_latest.geojson",
    "outputs/points_lite_peak_48_latest.geojson",
    "outputs/points_lite_peak_72_latest.geojson"
  )
  for (f in must_exist) if (!file.exists(f)) stop("Missing required artifact: ", f)

  # ensure each committed artifact < 100MB
  committed_patterns <- c(
    "outputs/latest.json",
    "outputs/available_runs.json",
    "outputs/points_lite_*_latest.geojson",
    "outputs/forecast_map_*.html",
    "outputs/*.pmtiles",
    "outputs/wrf_grid_forecast_*_nested.json"
  )
  files <- unique(unlist(lapply(committed_patterns, Sys.glob)))
  for (f in files) {
    sz <- file.info(f)$size / 1024^2
    message(sprintf("Size check: %s = %.2f MB", f, sz))
    if (is.na(sz) || sz > MAX_ARTIFACT_MB_GHA) stop("Artifact too large (>95MB): ", f)
  }

  message("All GHA artifacts produced and size-checked.")
}
