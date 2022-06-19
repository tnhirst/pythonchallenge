from attribute import Attribute
from industrial_buildings import IndustrialBuildings
import geopandas as gpd


class IndustrialFootprintInCatchment(Attribute):
    
    def __init__(self, catchment, osm_filename, *args, **kwargs):
        super().__init__({})
        self.catchment = catchment
        self.osm_filename = osm_filename

    
    def apply_to(self, grid):
        self.catchment.apply_catchment(grid)
        total_bounds = grid.data.set_geometry(self.catchment.catchment_name).to_crs('epsg:4326').total_bounds
        buildings_list = IndustrialBuildings(self.osm_filename, bounds=(total_bounds[0], total_bounds[1], total_bounds[2], total_bounds[3]), crs=grid.data.crs)
        buildings = gpd.GeoDataFrame({'geometry': buildings_list}, crs=grid.data.crs)
        overlays = gpd.overlay(grid.data.set_geometry(self.catchment.catchment_name), buildings)
        overlays[f'industrial_footprint_{self.catchment.catchment_name}'] = overlays.area
        overlays = overlays.groupby(['GRD_ID']).sum()
        grid.data = grid.data.merge(overlays, on='GRD_ID')

    def prepare(self):
        pass

