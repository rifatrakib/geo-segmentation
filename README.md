# geo-segmentation
Create geo-spatial grids with Turf.js and generate corresponding GeoJSON documents with geo-spatial information for MongoDB.

Polygons generated using turf.js are stored in `processed-small-grids.geojson` file to be used with standalone script as these are static data. The data can be built both by using the routes from `app.py` which will create the dataset incrementally one grid after another, or using the `utils.py` as a standalone script to get the data for all grids simply by running `python utils.py` in the terminal.
