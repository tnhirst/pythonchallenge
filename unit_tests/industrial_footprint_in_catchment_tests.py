from unittest import TestCase
from unittest.mock import patch, MagicMock
from industrial_footprint_in_catchment import IndustrialFootprintInCatchment 
import geopandas as gpd
from shapely.geometry import Polygon


class IndustrialFootprintInCatchmentTests(TestCase):
     
    @patch('industrial_footprint_in_catchment.IndustrialBuildings') 
    def test_attribute_includes_builing_in_catchment(self, industrial_buildings_mock):
        industrial_buildings_mock.return_value = [
            Polygon([[2, 2], [2, 3], [3, 3], [3, 2], [2, 2]])
        ]
        fake_catchment = FakeCatchment([
                Polygon([[0, 0],[0, 4],[4, 4],[4, 0],[0, 0]])
            ],
            '20min_drive')
        sut = IndustrialFootprintInCatchment(fake_catchment, 'source_osm_file')
        grid = MagicMock()
        grid.data = gpd.GeoDataFrame(
            {
                'geometry': [Polygon([[0, 0], [0, 2], [2, 2], [2, 0], [0, 0]])],
                'GRD_ID': ['1']
            }, 
            crs=3035)
        sut.apply_to(grid)
        self.assertEqual(1, grid.data['industrial_footprint_20min_drive'].iloc[0])

    @patch('industrial_footprint_in_catchment.IndustrialBuildings')
    def test_attribute_sums_floor_area_for_multiple_builings_in_catchment(self, industrial_buildings_mock):
        industrial_buildings_mock.return_value = [
            Polygon([[2, 2], [2, 3], [3, 3], [3, 2], [2, 2]]),
            Polygon([[2, 3], [2, 3.5], [3, 3.5], [3, 3], [2, 3]])
        ]
        fake_catchment = FakeCatchment([
                Polygon([[0, 0],[0, 4],[4, 4],[4, 0],[0, 0]])
            ], 
            '20min_drive')
        sut = IndustrialFootprintInCatchment(fake_catchment, 'source_osm_file')
        grid = MagicMock()
        grid.data = gpd.GeoDataFrame(
            {
                'geometry': [Polygon([[0, 0], [0, 2], [2, 2], [2, 0], [0, 0]])],
                'GRD_ID': ['1']
            },
            crs=3035)
        sut.apply_to(grid)
        self.assertEqual(1.5, grid.data['industrial_footprint_20min_drive'].iloc[0])
    
    @patch('industrial_footprint_in_catchment.IndustrialBuildings')
    def test_attribute_uses_industrial_buildings_from_bounding_box_defined_by_catchments(self, industrial_buildings_mock):
        industrial_buildings_mock.return_value = [
            Polygon([[2, 2], [2, 3], [3, 3], [3, 2], [2, 2]])
        ]
        fake_catchment = FakeCatchment([
                Polygon([[0, 0],[0, 4],[4, 4],[4, 0],[0, 0]]),
                Polygon([[0, 0],[0, 5],[3, 5],[3, 0],[0, 0]])
            ],
            '20min_drive')
        sut = IndustrialFootprintInCatchment(fake_catchment, 'source_osm_file')
        grid = MagicMock()
        grid.data = gpd.GeoDataFrame(
            {
                'geometry': [Polygon([[0, 0], [0, 2], [2, 2], [2, 0], [0, 0]]), Polygon([[2, 0], [2, 2], [4, 2], [4, 0], [2, 0]])],
                'GRD_ID': ['1', '2']
            },
            crs=4326)
        sut.apply_to(grid)
        args, kwargs = industrial_buildings_mock.call_args
        self.assertEqual((0, 0, 4, 5), kwargs['bounds'])


class FakeCatchment:
    
    def __init__(self, results, name):
        self.results = results
        self.catchment_name = name

    def apply_catchment(self, grid):
        grid.data[self.catchment_name] = self.results
