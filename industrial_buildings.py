from shapely.geometry import Polygon
from shapely.ops import transform 
from osmium import SimpleHandler
from osmium.geom import WKBFactory
import shapely.wkb as wkblib
import pyproj
import tempfile
import subprocess
from tqdm import tqdm


class IndustrialBuildings:
    
    def __init__(self, filename, bounds=None, crs=None):
        self.filename = filename
        self.bounds = bounds
        industrial_buildings_handler = IndustrialBuildingHandler()
        industrial_buildings_handler.apply_file(filename, locations=True)
        self.industrial_buildings = industrial_buildings_handler.results
        industrial_landuse_handler = IndustrialLanduseHandler(self.industrial_buildings)
        industrial_landuse_handler.apply_file(filename, locations=True)
        industrial_landuse_zones = industrial_landuse_handler.results
        buildings_in_industrial_zones_handler = BuildingsInZonesHandler(industrial_landuse_zones)
        buildings_in_industrial_zones_handler.apply_file(filename, locations=True)
        self.industrial_buildings.extend(buildings_in_industrial_zones_handler.results)
        self.transform = None
        if crs:
            crs4326 = pyproj.CRS("EPSG:4326")
            crs3035 = pyproj.CRS("EPSG:3035")
            self.transform = pyproj.Transformer.from_crs(crs4326, crs3035, always_xy=True).transform

    def __getitem__(self, index):
        building = self.industrial_buildings[index]
        if not self.transform:
            return building
        return transform(self.transform, building)
    
    def __len__(self):
        return len(self.industrial_buildings)


def way_is_in_zone(w, zone):
    bounds = zone.bounds
    nodes_in_bounds = [n for n in w.nodes if bounds[0] < n.x/10000000 < bounds[2] and bounds[1] < n.y/10000000 < bounds[3]]
    if len(nodes_in_bounds) == 0:
        return False
    if any([not hasattr(n, 'id') for n in w.nodes]):
        # This is an incomplete way from we generated the pbf extract
        return False
    way = Polygon([[n.x/10000000, n.y/10000000] for n in w.nodes])
    return way.intersects(zone)


class BuildingsInZonesHandler(SimpleHandler):
    
    def __init__(self, zones, total_ways=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.zones = zones
        self.wkbfab=WKBFactory()
        self.results=[]
        self.total_ways = total_ways
        self.pbar = None
    
    def way_is_in_a_zone(self, w):
        for zone in self.zones:
            if way_is_in_zone(w, zone):
                return True
        return False

    def apply_file(self, filename, locations=False):
        if self.total_ways:
            self.pbar = tqdm(total=self.total_ways)
        super().apply_file(filename, locations=locations)
        if self.total_ways:
            self.pbar.close()
    
    def node(self, n):
        pass
    
    def way(self, w):
        if "building" in w.tags and self.way_is_in_a_zone(w):
            self.results.append(Polygon([[n.x/10000000, n.y/10000000] for n in w.nodes]))
        if self.pbar:                                                                                                                                                                                                                                    self.pbar.update(1)
    
    def relation(self, r):                                                                                                                                                                                                                          pass


def building_is_in_way(building, way):
    x_coords = [n.x/10000000 for n in way.nodes]
    y_coords = [n.y/10000000 for n in way.nodes]
    bounds = (min(x_coords), min(y_coords), max(x_coords), max(y_coords))
    if not bounds[0] < building.centroid.x < bounds[2] and not bounds[1] < building.centroid.y < bounds[3]:
        return False
    geom = Polygon([[n.x/10000000, n.y/10000000] for n in way.nodes])
    return geom.intersects(building)


other_industrial_landuse_tags = ['commercial', 'industrial_park', 'harbour', 'logistics', 'port']
class IndustrialLanduseHandler(SimpleHandler):

    def __init__(self, industrial_buildings, total_ways=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.industrial_buildings = industrial_buildings
        self.wkbfab=WKBFactory()
        self.results=[]
        self.total_ways = total_ways
        self.pbar = None
    
    def way_contains_building(self, w):
        for b in self.industrial_buildings:
            if building_is_in_way(b, w):
                return True
        return False
    
    def apply_file(self, filename, locations=False):
        if self.total_ways:
            self.pbar = tqdm(total=self.total_ways)
        super().apply_file(filename, locations=locations)
        if self.total_ways:
            self.pbar.close()

    def node(self, n):
       pass

    def way(self, w):
       if self.pbar:                                                                                                                                                                                                                                    self.pbar.update(1)
       if "landuse" not in w.tags:
           return
       if len(w.nodes) < 3:
           return
       if w.tags["landuse"] == 'industrial':
           self.results.append(Polygon([[n.x/10000000, n.y/10000000] for n in w.nodes]))
           return
       if w.tags["landuse"] in other_industrial_landuse_tags and self.way_contains_building(w):
           self.results.append(Polygon([[n.x/10000000, n.y/10000000] for n in w.nodes]))

    def relation(self, r):
       pass


industrial_building_tags = ['industrial', 'warehouse', 'manufacture', 'factory', 'depot', 'works', 'workshop', 'industrial_unit']
class IndustrialBuildingHandler(SimpleHandler):
    
    def __init__(self, total_ways=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wkbfab=WKBFactory()
        self.results=[]
        self.total_ways = total_ways
        self.pbar = None
    
    def apply_file(self, filename, locations=False):
        if self.total_ways:
            self.pbar = tqdm(total=self.total_ways)
        super().apply_file(filename, locations=locations)
        if self.total_ways:
            self.pbar.close()

    def node(self, n):
       pass

    def way(self, w):
       if "building" in w.tags and w.tags["building"] in industrial_building_tags:
           self.results.append(Polygon([[n.x/10000000, n.y/10000000] for n in w.nodes]))
       if self.pbar:
           self.pbar.update(1)

    def relation(self, r):
       pass
