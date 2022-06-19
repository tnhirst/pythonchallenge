from unittest import TestCase
from unittest.mock import patch, MagicMock
from shapely.geometry import Polygon
from osmium import SimpleHandler
from osmium.osm.mutable import Way
from ddt import ddt, data
import sys

fake_ways = []
nelsons_column_osm = Polygon([[-1280230, 515076570], [-1277930, 515076970], [-1278520, 515078370], [-1280820, 515077980], [-1280230, 515076570]])
nelsons_column_3035 = Polygon([[3620426.652012135, 3203933.864447915], [3620443.05812613, 3203936.06868994], [3620441.15939868, 3203952.085517246], [3620424.768703199, 3203949.991639197], [3620426.652012135, 3203933.864447915]])
trafalgar_square_osm = Polygon([[-1286880, 515074440], [-1269660, 515076690], [-1274140, 515086160], [-1290530, 515082980], [-1286880, 515074440]])
fourth_plinth_osm = Polygon([[-1287150, 515081680], [-1286840, 515081730], [-1287010, 515082150], [-1287290, 515082110], [-1287150, 515081680]])
fourth_plinth_3035 = Polygon([[3620386.996741111, 3203996.906134959], [3620389.201964355, 3203997.160024315], [3620388.680477633, 3204001.958352639], [3620386.696599414, 3204001.785991563], [3620386.996741111, 3203996.906134959]])

def mockNode(node, locations):
    nodeMock = MagicMock()
    if locations:
        nodeMock.x = node[0]
        nodeMock.y = node[1]
    return nodeMock


class FakeHandler(SimpleHandler):

    def apply_file(self, filename, locations=False):
        osm_id = 0
        for way in fake_ways:
            for p in way['geometry'].exterior.coords:
                self.node(p)
        for way in fake_ways:
            osm_id = osm_id + 1
            mock_way = MagicMock()
            mock_way.id = osm_id
            mock_way.tags=way['tags']
            mock_way.nodes=[mockNode(n, locations) for n in way['geometry'].exterior.coords]
            self.way(mock_way)
 
   

@ddt
class TestIndustrialBuildings(TestCase):

    # TODO: Test what happens on invalid geometries, including linear ring with two points
    
    def setUp(self):
        global fake_ways
        global IndustrialBuildings
        fake_ways = []
        if 'industrial_buildings' in sys.modules:
            del sys.modules['industrial_buildings']
        patcher = patch('osmium.SimpleHandler', new=FakeHandler)
        patcher.start()
        from industrial_buildings import IndustrialBuildings
        self.addCleanup(patcher.stop)
        self.subprocess_patch = patch('industrial_buildings.subprocess')
        self.subprocess_patch.start()
        self.addCleanup(self.subprocess_patch.stop)
    
    def tearDown(self):
        del sys.modules['industrial_buildings']

    @data('industrial', 'warehouse', 'manufacture', 'factory', 'depot', 'works', 'workshop', 'industrial_unit')
    def test_returns_building_with_industrial_type_building_tag(self, tag):
        fake_ways.append({'geometry': nelsons_column_osm, 'tags': {'building': tag}})
        bounds = {}
        sut = IndustrialBuildings('osm_file', bounds=bounds, crs="EPSG:3035")
        self.assertTrue(sut[0].almost_equals(nelsons_column_3035))
    
    def test_does_not_return_building_with_no_industrial_type_tag(self):
        fake_ways.append({'geometry': nelsons_column_osm, 'tags': {'building': 'yes'}})
        bounds = {}
        sut = IndustrialBuildings('osm_file', bounds=bounds, crs="EPSG:3035")
        self.assertEqual(0, len(sut))
    
    def test_returns_building_in_industrial_landuse(self):
        fake_ways.append({'geometry': nelsons_column_osm, 'tags': {'building': 'yes'}})
        fake_ways.append({'geometry': trafalgar_square_osm, 'tags': {'landuse': 'industrial'}})
        bounds = {}
        sut = IndustrialBuildings('osm_file', bounds=bounds, crs='EPSG:3035')
        self.assertTrue(sut[0].almost_equals(nelsons_column_3035))

    @data('commercial', 'harbour', 'industrial_park', 'logistics', 'port')
    def test_returns_buildings_in_certain_landuses_that_also_have_industrial_buildings(self, landuse_tag):
        fake_ways.append({'geometry': nelsons_column_osm, 'tags': {'building': 'yes'}})
        fake_ways.append({'geometry': fourth_plinth_osm, 'tags': {'building': 'industrial'}})
        fake_ways.append({'geometry': trafalgar_square_osm, 'tags': {'landuse': landuse_tag}})
        bounds = {}
        sut = IndustrialBuildings('osm_file', bounds=bounds, crs='EPSG:3035')
        self.assertTrue(next((b for b in sut if b.almost_equals(nelsons_column_3035)), None))
        self.assertTrue(next((b for b in sut if b.almost_equals(fourth_plinth_3035)), None))

    @data('commercial', 'harbour', 'industrial_park', 'logistics', 'port')
    def test_does_not_return_buildings_in_certain_landuses_that_do_not_also_have_industrial_buildings(self, landuse_tag):
        fake_ways.append({'geometry': nelsons_column_osm, 'tags': {'building': 'yes'}})
        fake_ways.append({'geometry': fourth_plinth_osm, 'tags': {'building': 'yes'}})
        fake_ways.append({'geometry': trafalgar_square_osm, 'tags': {'landuse': landuse_tag}})
        bounds = {}
        sut = IndustrialBuildings('osm_file', bounds=bounds, crs='EPSG:3035')
        self.assertEqual(0, len(sut))


