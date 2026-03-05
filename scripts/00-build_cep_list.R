###############################################################
# 00_build_cep_list.R
#
# ONE-TIME script: pull every real assigned CEP for SC directly
# from geocodebr's local CNEFE Parquet cache, batch-geocode them,
# deduplicate by coordinate, and save:
#
#   outputs/cep_list.rds     <- loaded by main forecast script
#   outputs/cep_list.csv     <- human-readable / editable
#   outputs/cep_list.geojson <- inspect in QGIS / Lovable
#
# Why this approach is better than the Excel range enumeration:
#   - Uses only CEPs that ACTUALLY EXIST in CNEFE (no misses)
#   - No guessing / stepping through ranges
#   - No external file dependency
#   - geocodebr already has the data locally — zero extra downloads
#   - busca_por_cep() accepts a VECTOR, so one call does the lot
#
# Pre-requisite: run geocodebr::download_cnefe() at least once
# (or let geocode() / busca_por_cep() trigger it automatically).
# The CNEFE cache is ~1-2 GB but only downloads once.
###############################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(sf)
  library(arrow)      # read Parquet files
  library(geocodebr)
})

`%||%` <- function(a, b) if (!is.null(a)) a else b

# =============================================================
# CONFIGURATION
# =============================================================

UF_FILTER  <- "SC"          # state abbreviation
OUTPUT_DIR <- "outputs"     # where to save the results

# Optional: keep only specific municipalities (NULL = all SC)
# Example: MUNICIPIO_FILTER <- c("Joinville", "Florianópolis")
MUNICIPIO_FILTER <- NULL

# =============================================================
# STEP 1 — Make sure CNEFE data is cached locally
# =============================================================

cat("================================================================\n")
cat("WRF CEP List Builder — CNEFE direct query\n")
cat("================================================================\n\n")

cat("Checking geocodebr CNEFE cache...\n")

# This triggers download if not yet cached (~1 GB, one-time only)
# Suppress the verbose output from geocodebr itself
cached <- tryCatch(
  geocodebr::listar_dados_cache(),
  error = function(e) character(0)
)

if (!length(cached)) {
  cat("  CNEFE not yet downloaded. Downloading now (one-time, ~1 GB)...\n")
  geocodebr::download_cnefe(verboso = TRUE)
  cached <- geocodebr::listar_dados_cache()
}

cat(sprintf("  %d Parquet files in cache.\n", length(cached)))

# =============================================================
# STEP 2 — Read CEPs directly from the Parquet cache
#
# geocodebr caches several Parquet files; the one that maps
# CEP -> municipio -> estado is  municipio_cep.parquet
# (or municipio_cep_localidade.parquet for bairro detail).
# We prefer the localidade file for richer labels.
# =============================================================

cat("\nLocating best CEP Parquet file...\n")

# Try richest file first, fall back to simpler ones
preferred <- c("municipio_logradouro_cep_localidade",
               "municipio_cep_localidade",
               "municipio_cep")

parquet_path <- NULL
for (pat in preferred) {
  hit <- cached[grepl(pat, cached, fixed = TRUE)]
  if (length(hit)) { parquet_path <- hit[1]; break }
}

if (is.null(parquet_path))
  stop("No CEP Parquet file found in geocodebr cache.\n",
       "Run geocodebr::download_cnefe() first.\n",
       "Cached files:\n", paste(cached, collapse = "\n"))

cat(sprintf("  Using: %s\n", basename(parquet_path)))

# Read and filter to target state
cat(sprintf("  Reading and filtering to UF=%s...\n", UF_FILTER))

ds <- arrow::open_dataset(parquet_path)

# Inspect columns
all_cols <- names(ds)
cat(sprintf("  Columns: %s\n", paste(all_cols, collapse=", ")))

# Identify key columns (geocodebr uses Portuguese names)
estado_col <- intersect(c("estado","uf","UF","ESTADO"), all_cols)[1]
mun_col    <- intersect(c("municipio","MUNICIPIO","nm_municipio"), all_cols)[1]
cep_col    <- intersect(c("cep","CEP"), all_cols)[1]
bairro_col <- intersect(c("localidade","bairro","LOCALIDADE","BAIRRO"), all_cols)[1]

if (is.na(estado_col) || is.na(cep_col))
  stop("Cannot find estado or cep column. Available: ", paste(all_cols, collapse=", "))

cat(sprintf("  Columns mapped: estado=%s, municipio=%s, cep=%s, bairro=%s\n",
            estado_col, mun_col %||% "N/A", cep_col, bairro_col %||% "N/A"))

# Build filter expression dynamically
query <- ds |>
  dplyr::filter(.data[[estado_col]] == UF_FILTER)

if (!is.na(mun_col) && !is.null(MUNICIPIO_FILTER)) {
  query <- query |> dplyr::filter(.data[[mun_col]] %in% MUNICIPIO_FILTER)
}

# Select relevant columns and collect
sel_cols <- c(estado_col, mun_col, cep_col, bairro_col)
sel_cols <- sel_cols[!is.na(sel_cols)]

cep_raw <- query |>
  dplyr::select(dplyr::all_of(sel_cols)) |>
  dplyr::distinct() |>
  dplyr::collect()

# Rename to standard names
rename_map <- stats::setNames(
  sel_cols,
  c("estado",
    if (!is.na(mun_col))    "municipio"  else NULL,
    "cep",
    if (!is.na(bairro_col)) "localidade" else NULL)
)
cep_raw <- dplyr::rename(cep_raw, dplyr::all_of(rename_map))

# Ensure CEP is 8-char zero-padded string
cep_raw$cep <- formatC(as.integer(gsub("\\D","",cep_raw$cep)),
                       width=8L, flag="0")
cep_raw <- cep_raw[!is.na(cep_raw$cep) & nchar(cep_raw$cep)==8, ]
cep_raw <- dplyr::distinct(cep_raw, cep, .keep_all=TRUE)

cat(sprintf("\n  %d unique CEPs found for UF=%s\n", nrow(cep_raw), UF_FILTER))
if (!is.null(MUNICIPIO_FILTER))
  cat(sprintf("  (filtered to %d municipalities)\n", length(MUNICIPIO_FILTER)))

# =============================================================
# STEP 3 — Batch geocode with busca_por_cep()
#
# busca_por_cep() accepts a VECTOR of CEPs in one call and
# reads entirely from the local CNEFE cache — no HTTP requests.
# For ~tens of thousands of CEPs this takes a few minutes.
# =============================================================

cat(sprintf("\nGeocoding %d CEPs via busca_por_cep()...\n", nrow(cep_raw)))
cat("  (reads from local cache — no internet needed)\n")

t0 <- Sys.time()

geo <- geocodebr::busca_por_cep(
  cep         = cep_raw$cep,
  resultado_sf = FALSE,
  verboso      = TRUE,
  cache        = TRUE
)

elapsed <- as.numeric(difftime(Sys.time(), t0, units="mins"))
cat(sprintf("\n  Geocoding finished in %.1f min\n", elapsed))

# =============================================================
# STEP 4 — Clean, fill metadata, deduplicate
# =============================================================

cat("\nAssembling results...\n")
cat("  busca_por_cep() output columns: ", paste(names(geo), collapse=", "), "\n")

# Identify coordinate columns
lon_col <- intersect(c("lon","longitude","x"), names(geo))[1]
lat_col <- intersect(c("lat","latitude","y"),  names(geo))[1]
if (is.na(lon_col) || is.na(lat_col))
  stop("No lon/lat in busca_por_cep output. Columns: ",
       paste(names(geo), collapse=", "))

# Drop rows without coordinates
geo_ok <- geo[!is.na(geo[[lon_col]]) & !is.na(geo[[lat_col]]), ]
cat(sprintf("  %d / %d CEPs resolved (%.1f%%)\n",
            nrow(geo_ok), nrow(geo),
            100 * nrow(geo_ok) / max(nrow(geo), 1)))

# Normalise CEP to 8-digit string in geo_ok
cep_col_geo <- intersect(c("cep","CEP"), names(geo_ok))[1]
geo_ok[[cep_col_geo]] <- formatC(
  as.integer(gsub("\\D","", geo_ok[[cep_col_geo]])),
  width = 8L, flag = "0")

# Also normalise in cep_raw so the join key matches exactly
cep_raw$cep <- formatC(
  as.integer(gsub("\\D","", cep_raw$cep)),
  width = 8L, flag = "0")

# ------------------------------------------------------------------
# Strategy: busca_por_cep() already returns municipio / localidade
# columns in its output.  Use those directly when available.
# Only fall back to the Parquet join when they are missing/all-NA.
# ------------------------------------------------------------------

# Detect municipio column in geo output (may be named municipio,
# nm_municipio, city, cidade, etc.)
mun_col_geo  <- intersect(
  c("municipio","nm_municipio","city","cidade","MUNICIPIO"), names(geo_ok))[1]
loc_col_geo  <- intersect(
  c("localidade","bairro","neighborhood","LOCALIDADE","BAIRRO"), names(geo_ok))[1]

use_geo_mun  <- !is.na(mun_col_geo) &&
  sum(!is.na(geo_ok[[mun_col_geo]])) > 0
use_geo_loc  <- !is.na(loc_col_geo) &&
  sum(!is.na(geo_ok[[loc_col_geo]])) > 0

cat(sprintf("  municipio from geocodebr output : %s (%s)\n",
            if (use_geo_mun) "YES" else "NO — will join from Parquet",
            mun_col_geo %||% "not found"))
cat(sprintf("  localidade from geocodebr output: %s (%s)\n",
            if (use_geo_loc) "YES" else "NO — will join from Parquet",
            loc_col_geo %||% "not found"))

if (use_geo_mun && use_geo_loc) {
  # Happy path: geocodebr output already has everything we need
  cep_df <- data.frame(
    municipio  = as.character(geo_ok[[mun_col_geo]]),
    localidade = as.character(geo_ok[[loc_col_geo]]),
    cep        = geo_ok[[cep_col_geo]],
    lon        = as.numeric(geo_ok[[lon_col]]),
    lat        = as.numeric(geo_ok[[lat_col]]),
    stringsAsFactors = FALSE
  )
} else {
  # Fallback: join back to cep_raw from the Parquet query.
  # Both sides are now guaranteed to be clean 8-digit strings.
  cat("  Joining to Parquet metadata...\n")
  merged <- dplyr::left_join(
    geo_ok,
    cep_raw[, c("cep",
                if ("municipio"  %in% names(cep_raw)) "municipio"  else NULL,
                if ("localidade" %in% names(cep_raw)) "localidade" else NULL)],
    by = setNames("cep", cep_col_geo)
  )

  cep_df <- data.frame(
    municipio  = if ("municipio"  %in% names(merged)) as.character(merged$municipio)
    else NA_character_,
    localidade = if ("localidade" %in% names(merged)) as.character(merged$localidade)
    else NA_character_,
    cep        = merged[[cep_col_geo]],
    lon        = as.numeric(merged[[lon_col]]),
    lat        = as.numeric(merged[[lat_col]]),
    stringsAsFactors = FALSE
  )
}

# Report how many still have NA municipio after both strategies
n_na_mun <- sum(is.na(cep_df$municipio))
if (n_na_mun > 0)
  cat(sprintf("  Warning: %d rows still have NA municipio (CEPs not in CNEFE index)\n",
              n_na_mun))

cep_df$cep_fmt <- paste0(substr(cep_df$cep,1,5),"-",substr(cep_df$cep,6,8))

# Build human label
cep_df$label <- dplyr::case_when(
  !is.na(cep_df$localidade) & nzchar(trimws(cep_df$localidade)) &
    !is.na(cep_df$municipio) ~
    paste0(cep_df$municipio, " \u2013 ", trimws(cep_df$localidade),
           " (", cep_df$cep_fmt, ")"),
  !is.na(cep_df$municipio) ~
    paste0(cep_df$municipio, " (", cep_df$cep_fmt, ")"),
  TRUE ~ cep_df$cep_fmt   # last resort: just the CEP
)

# Remove rows with bad coordinates
cep_df <- cep_df[is.finite(cep_df$lon) & is.finite(cep_df$lat), ]

# Deduplicate identical (lon,lat) within same municipality
n_before <- nrow(cep_df)
cep_df <- cep_df[!duplicated(
  paste(cep_df$municipio,
        round(cep_df$lon, 4),
        round(cep_df$lat, 4))
), ]
cat(sprintf("  %d duplicate coordinates removed\n", n_before - nrow(cep_df)))
cat(sprintf("  %d unique geocoded points retained\n", nrow(cep_df)))

# Summary
mun_summary <- cep_df |>
  dplyr::count(municipio, name="n_points") |>
  dplyr::arrange(dplyr::desc(n_points))

cat("\nTop 10 municipalities by point density:\n")
print(head(mun_summary, 10))
cat(sprintf("\nMedian points per municipality: %.0f\n",
            median(mun_summary$n_points)))

# =============================================================
# STEP 5 — Save outputs
# =============================================================

cat("\nSaving outputs...\n")
dir.create(OUTPUT_DIR, showWarnings=FALSE, recursive=TRUE)

rds_path <- file.path(OUTPUT_DIR, "cep_list.rds")
saveRDS(cep_df, rds_path)
cat(sprintf("  RDS     : %s  (%d rows)\n", rds_path, nrow(cep_df)))

csv_path <- file.path(OUTPUT_DIR, "cep_list.csv")
utils::write.csv(cep_df, csv_path, row.names=FALSE, fileEncoding="UTF-8")
cat(sprintf("  CSV     : %s\n", csv_path))

cep_sf <- sf::st_as_sf(cep_df, coords=c("lon","lat"), crs=4326, remove=FALSE)
gj_path <- file.path(OUTPUT_DIR, "cep_list.geojson")
sf::st_write(cep_sf, gj_path, driver="GeoJSON",
             layer_options="RFC7946=YES",
             delete_dsn=TRUE, quiet=TRUE)
cat(sprintf("  GeoJSON : %s\n", gj_path))

cat("\n================================================================\n")
cat("Done.\n")
cat(sprintf("  UF            : %s\n",  UF_FILTER))
cat(sprintf("  Total points  : %d\n",  nrow(cep_df)))
cat(sprintf("  Municipalities: %d\n",  length(unique(cep_df$municipio))))
cat(sprintf("  Elapsed       : %.1f min\n", elapsed))
cat("\nTo use in the forecast script, set:\n")
cat(sprintf('  CEP_LIST_PATH <- "%s"\n', rds_path))
cat("================================================================\n")

