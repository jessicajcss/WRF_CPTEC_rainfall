###############################################################
# WRF / CPTEC – PMTiles export
#
# Reads the nested GeoJSON produced by 00-geojson_output.R and
# builds two point layers:
#
#   cep_rain_now.pmtiles  — rain_mm at the first valid forecast hour
#   cep_peak_24.pmtiles   — peak rain_mm over forecast hours 1–24
#
# Both files are written to outputs/ at zoom levels Z5–Z12.
#
# Usage (standalone):
#   Rscript scripts/00-pmtiles_output.R
#
# Usage (called from the same session that ran 00-geojson_output.R):
#   The script will detect df_multi in the parent environment and
#   use it directly, skipping the re-load step.
#
# Requires: sf, dplyr, jsonlite
#           tippecanoe must be on PATH
###############################################################

suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
})

# Local definition — do not rely on the global env
`%||%` <- function(a, b) if (!is.null(a)) a else b

# =============================================================
# 1.  Obtain df_multi
# =============================================================

if (!exists("df_multi")) {
  # Re-load from the most recent nested GeoJSON in outputs/
  candidates <- Sys.glob("outputs/wrf_cep_forecast_*_nested.geojson")
  if (!length(candidates)) {
    stop("No outputs/wrf_cep_forecast_*_nested.geojson found. ",
         "Run 00-geojson_output.R first.")
  }
  # Pick newest by modification time
  mtimes     <- file.info(candidates)$mtime
  geojson_path <- candidates[which.max(mtimes)]
  cat(sprintf("Loading GeoJSON: %s\n", geojson_path))
  df_multi <- sf::st_read(geojson_path, quiet = TRUE)
} else {
  cat("Using df_multi from the calling session.\n")
}

# =============================================================
# 2.  Un-nest parallel arrays → long-format data frame
# =============================================================
# df_multi has one row per CEP.  The time-series columns
# (forecast_hour, valid_utc, rain_mm, accum_mm, rain_class)
# are stored as JSON arrays inside each feature.

# Strip geometry once — avoids repeated geometry operations in the loop
df_props <- sf::st_drop_geometry(df_multi)

unnest_cep <- function(props) {
  # Helper: safely parse a JSON array column to an R vector
  parse_col <- function(col_name) {
    val <- props[[col_name]]
    if (is.null(val)) return(NULL)
    if (is.list(val)) val <- val[[1]]
    if (is.character(val) && length(val) == 1) {
      tryCatch(jsonlite::fromJSON(val), error = function(e) NULL)
    } else {
      val
    }
  }

  fh   <- parse_col("forecast_hour")
  vu   <- parse_col("valid_utc")
  rm_  <- parse_col("rain_mm")
  am   <- parse_col("accum_mm")
  rc   <- parse_col("rain_class")

  n <- max(lengths(list(fh, vu, rm_, am, rc)), 0L)
  if (n == 0L) return(NULL)

  pad <- function(x, len) {
    if (is.null(x) || length(x) == 0) return(rep(NA, len))
    if (length(x) < len) c(x, rep(NA, len - length(x))) else x
  }

  data.frame(
    cep           = props$cep          %||% NA_character_,
    municipio     = props$municipio    %||% NA_character_,
    label         = props$label        %||% NA_character_,
    lon           = props$lon          %||% NA_real_,
    lat           = props$lat          %||% NA_real_,
    init_utc      = props$init_utc     %||% NA_character_,
    forecast_hour = pad(fh,  n),
    valid_utc     = pad(vu,  n),
    rain_mm       = as.numeric(pad(rm_, n)),
    accum_mm      = as.numeric(pad(am,  n)),
    rain_class    = pad(rc,  n),
    stringsAsFactors = FALSE
  )
}

cat("Un-nesting CEP rows …\n")
df_long <- dplyr::bind_rows(lapply(seq_len(nrow(df_props)), function(i) {
  unnest_cep(df_props[i, ])
}))

cat(sprintf("  Long-format rows: %d\n", nrow(df_long)))

# =============================================================
# 3.  sf_now — rain at the first non-NA forecast hour
# =============================================================
cat("Building sf_now …\n")

df_now <- df_long %>%
  filter(!is.na(forecast_hour), !is.na(rain_mm)) %>%
  group_by(cep) %>%
  slice_min(order_by = forecast_hour, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  select(cep, municipio, label, lon, lat,
         forecast_hour, valid_utc, rain_mm, rain_class)

sf_now <- sf::st_as_sf(df_now,
                       coords = c("lon", "lat"),
                       crs    = 4326,
                       remove = FALSE)

cat(sprintf("  sf_now: %d features\n", nrow(sf_now)))

# =============================================================
# 4.  sf_peak24 — peak rain over hours 1–24
# =============================================================
cat("Building sf_peak24 …\n")

df_peak24 <- df_long %>%
  filter(!is.na(forecast_hour),
         forecast_hour >= 1,
         forecast_hour <= 24) %>%
  group_by(cep) %>%
  summarise(
    municipio      = first(municipio),
    label          = first(label),
    lon            = first(lon),
    lat            = first(lat),
    init_utc       = first(init_utc),
    peak_rain_mm   = max(rain_mm,   na.rm = TRUE),
    peak_hour      = forecast_hour[which.max(rain_mm)],
    peak_valid_utc = valid_utc[which.max(rain_mm)],
    total_rain_mm  = sum(rain_mm,   na.rm = TRUE),
    n_rainy_hours  = sum(rain_mm > 0, na.rm = TRUE),
    rain_class     = rain_class[which.max(rain_mm)],
    .groups = "drop"
  )

sf_peak24 <- sf::st_as_sf(df_peak24,
                           coords = c("lon", "lat"),
                           crs    = 4326,
                           remove = FALSE)

cat(sprintf("  sf_peak24: %d features\n", nrow(sf_peak24)))

# =============================================================
# 5.  Write PMTiles via tippecanoe
# =============================================================
dir.create("outputs", showWarnings = FALSE, recursive = TRUE)

run_tippecanoe <- function(sf_obj, layer_name, out_pmtiles) {
  tmp_geojson <- tempfile(fileext = ".geojson")
  on.exit(unlink(tmp_geojson), add = TRUE)

  sf::st_write(sf_obj, tmp_geojson, driver = "GeoJSON",
               delete_dsn = TRUE, quiet = TRUE)

  cmd <- sprintf(
    "tippecanoe --no-tile-compression -z12 -Z5 -l %s -o %s %s --force",
    shQuote(layer_name),
    shQuote(out_pmtiles),
    shQuote(tmp_geojson)
  )
  cat(sprintf("Running: %s\n", cmd))
  ret <- system(cmd)
  if (ret != 0L) stop(sprintf("tippecanoe failed (exit %d) for %s", ret, layer_name))
  invisible(out_pmtiles)
}

out_now   <- "outputs/cep_rain_now.pmtiles"
out_peak  <- "outputs/cep_peak_24.pmtiles"

run_tippecanoe(sf_now,   "cep_rain_now", out_now)
run_tippecanoe(sf_peak24, "cep_peak_24",  out_peak)

# =============================================================
# 6.  Summary
# =============================================================
cat("\n--- PMTiles output summary ---\n")
for (f in c(out_now, out_peak)) {
  if (file.exists(f)) {
    sz <- file.info(f)$size
    cat(sprintf("  %-40s  %s\n", f,
                if (sz > 1e6) sprintf("%.1f MB", sz / 1e6)
                else           sprintf("%.1f KB", sz / 1e3)))
  } else {
    cat(sprintf("  %-40s  MISSING\n", f))
  }
}
cat("-------------------------------\n")
