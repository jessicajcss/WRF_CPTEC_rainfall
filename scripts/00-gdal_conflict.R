# Run this in your R console
terra::gdal()           # shows GDAL version terra is using
sf::sf_extSoftVersion() # shows GDAL/PROJ versions sf sees

# Find where the data files SHOULD be
gdal_data <- system.file("gdal", package = "terra")
proj_lib  <- system.file("proj", package = "terra")
cat("GDAL data:", gdal_data, "\n")
cat("PROJ lib :", proj_lib,  "\n")


# Force GDAL and PROJ to use the data bundled with terra
# (prevents conflicts with OSGeo4W / QGIS / conda installations)
gdal_data_path <- system.file("gdal", package = "terra")
proj_lib_path  <- system.file("proj", package = "terra")

if (nzchar(gdal_data_path)) Sys.setenv(GDAL_DATA = gdal_data_path)
if (nzchar(proj_lib_path))  Sys.setenv(PROJ_LIB  = proj_lib_path)
