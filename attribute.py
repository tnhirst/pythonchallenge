from abc import ABC, abstractmethod
import psycopg2
from sklearn.neighbors import BallTree
import numpy as np
import pandas as pd
import geopandas as gpd
from typing import Union, List, Tuple, Iterable
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

from grid_dataset import GridDataset


class Attribute(ABC):
    """
    A class used to represent any location-scoring attribute.

    A location-scoring tool attribute is a measurable fact about a location. Each attribute is derived from one or
    more source datasets. An attribute has, by definition, units. An example of an attribute is the workforce within
    50-minute drive or public transport journey. An attribute may be used in the derivation of one or more suitability
    parameter scores, or may just be derived for presentation in its own right, and therefore not fed into any
    suitability parameter score.

    Attributes
    ----------
    name: str
        unique name to identify each attribute
    title: str
        "pretty" name to be used to identify/describe attribute to an end user
    config: dict
        config containing all environment-specific variables
    ttp_service: TravelTimeService
        travel time service object from which to extract travel time catchments
    datasets: list of Dataset
        datasets from which the attribute is calculated
    catchment: Catchment
        catchment used in calculation of the attribute
    col: str
        name of column in dataset from which the attribute is calculated
    travel_time: int
        travel time (in s) for catchment if time-based catchment is used
    dist: int
        travel distance (in s) for catchment if distance-based catchment is used or if time-based catchment is used
        then travel_time * 120 km/h (converted to m/s)
    val_range: list of float
        range of this attribute in this deployment [min, max]
    layer_id: str
        unique id of this attributes layer in the layers table of the deployments database

    Methods
    -------
    add_ttp_service(service: TravelTimeService)
        By default, does nothing. Child classes that require TTP service overwrite this method to add it to self.
    prepare()
        Prepares all required class attributes for calculation.
    apply_to(grid: GridDataset)
        Applies the attribute's data to a grid object.
    make_datasets(use_case: UseCase)
        Creates/prepares dataset objects required for this attribute that do not already exist for the use case.
    make_fresh_catchment()
        Recreates the attribute's catchment due to an issue where it disappears during multiprocessing.
    add_col(db_config: dict)
        Adds column for the attribute to the attributes table in the database.
    get_nearest(src_points: Iterable[Tuple[float, float]], candidates: Iterable[Tuple[float, float]],
                k_neighbors: int = 1)
        For each point in src_points, returns the nearest candidate in candidates and distance to the candidate.
    nearest_neighbor(left_data: Union[gpd.GeoDataFrame, List[BaseGeometry]],
                        right_data: Union[gpd.GeoDataFrame, List[BaseGeometry]], crs: int)
        For each geometry in left_data, finds the nearest geometry in right data and distance to it.
    """

    def __init__(self, config: dict) -> None:
        """
        Parameters
        ----------
        config: dict
            Config containing all environment-specific variables
        """

        self.name = None
        self.title = None
        self.config = config
        self.ttp_service = None
        # Objects required by this attribute
        self.datasets = []
        self.catchment = None
        # Values used by other objects in relation to an attribute
        self.col = None
        self.travel_time = None
        self.dist = 0
        # Values needed when running generate_tiles
        self.val_range = None
        self.layer_id = None


    @abstractmethod
    def prepare(self):
        """
        Prepares all required class attributes for calculation.

        Outlines required datasets, catchments, columns to search for, travel time, etc., i.e., all attribute-specific
        attributes required to completely calculate the attribute.
        """
        pass

    @abstractmethod
    def apply_to(self, grid: GridDataset) -> None:
        """
        Applies the attribute's data to a grid object.

        Parameters
        ----------
        grid: GridDataset
            Grid object to which the attribute is to be applied to.
        """
        pass


    def _apply_catchment_to_grid(self, grid: GridDataset) -> None:
        """
        Applies attribute's required catchments to the grid object.

        Parameters
        ----------
        grid: GridDataset
            Grid object to which the attribute is to be applied to.
        """
        # TODO Unit test attribute appends itself to pre-existing catchment
        self.make_fresh_catchment()
        existing_catchment = next((c for c in grid.catchments if c.catchment_name ==
                                   self.catchment.catchment_name), None)
        if existing_catchment is not None:
            existing_catchment.append(self.catchment)
            self.catchment = existing_catchment
        else:
            self.catchment.apply_catchment(grid)
            grid.catchments.append(self.catchment)

    def make_fresh_catchment(self) -> None:
        """
        Recreates the attribute's catchment due to an issue where it disappears during multiprocessing.
        """
        # TODO this method exists as catchments "persist" in the attribute objects between sub grids duplicating the
        #   columns to aggregate over. This will be resolved once catchments are applied to the grid before splitting.
        #   However, GPKGs can only have one geometry column which presents a problem. Will probably resolve using WKT.
        pass

    def add_col(self, db_config: dict) -> None:
        """
        Adds column for the attribute to the attributes table in the database.

        Parameters
        ----------
        db_config: dict
            database connection details from main config dict
        """
        with psycopg2.connect(
                user=db_config["username"],
                password=db_config["password"],
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["database"]) as conn, conn.cursor() as cur:
            cur.execute("ALTER TABLE attributes ADD COLUMN {} double precision;".format(self.name))
            conn.commit()

    @staticmethod
    def get_nearest(src_points: Iterable[Tuple[float, float]], candidates: Iterable[Tuple[float, float]],
                    k_neighbors: int = 1) -> Tuple[List[int], List[float]]:
        """
        For each point in src_points, returns the nearest candidate in candidates and distance to the candidate.

        Parameters
        ----------
        src_points: Iterable[Tuple[float, float]]
            Iterable of points from which to find nearest neighbours
        candidates: Iterable[Tuple[float, float]]
            Iterable of points to find nearest of for each point in src_points
        k_neighbors: int
            Which rank neighbour to find: 1 being closest, 2 second closest and so on (default is 1)

        Returns
        -------
        (list[int], list[float])
            Nearest candidate indices for each point in src_points as well as distance to the candidate
        """

        # Create tree from the candidate points
        tree = BallTree(candidates, leaf_size=15)

        # Find closest points and distances
        distances, indices = tree.query(src_points, k=k_neighbors)

        # Transpose to get distances and indices into arrays
        distances = distances.transpose()
        indices = indices.transpose()

        # Get closest indices and distances (i.e. array at index 0)
        # note: for the second closest points, you would take index 1, etc.
        closest = indices[0]
        closest_dist = distances[0]

        # Return indices and distances
        return closest, closest_dist

    def nearest_neighbor(self, left_data: Union[gpd.GeoDataFrame, List[BaseGeometry]],
                         right_data: Union[gpd.GeoDataFrame, List[BaseGeometry]],
                         crs: int) -> Tuple[List[BaseGeometry], List[float]]:
        """
        For each geometry in left_data, finds the nearest geometry in right data and distance to it.

        Parameters
        ----------
        left_data: Union[GeoDataFrame, list of BaseGeometry]
            Data for which to find nearest neighbours for
        right_data: Union[GeoDataFrame, list of BaseGeometry]
            Candidate neighbours
        crs: int
            Coordinate reference system in use

        Returns
        -------
        (list of BaseGeometry, list of float)
            Nearest candidate for each point in left_data as well as distance to the candidate
        """

        if not len(right_data):
            return [np.nan] * len(left_data), [np.inf] * len(left_data)

        left_coords = self._get_centroid_coords(left_data, crs)
        right_coords = self._get_centroid_coords(right_data, crs)

        if isinstance(right_data, pd.DataFrame):
            right = right_data.copy().reset_index(drop=True)
            closest, dist = self.get_nearest(src_points=left_coords, candidates=right_coords)
            closest_points = right.loc[closest]
            closest_points = closest_points.reset_index(drop=True)
        else:
            closest, dist = self.get_nearest(src_points=left_coords, candidates=right_coords)
            closest_points = [right_data[i] for i in closest]

        if crs == 4326:
            earth_radius = 6371000  # meters
            dist = [d * earth_radius for d in dist]

        return closest_points, dist

    @staticmethod
    def _get_centroid_coords(data: Union[gpd.GeoDataFrame, List[BaseGeometry]],
                             crs: int) -> Iterable[Tuple[float, float]]:
        """
        Converts a list of shapes or the geometry column of a geodataframe into a list of centroids as coordinates.

        Parameters
        ----------
        data: Union[gpd.GeoDataFrame, List[BaseGeometry]]
            Data for which to find each geometry's centroid's coordinates
        crs: int
            Coordinate reference system in use

        Returns
        -------
        Iterable[Tuple[float, float]]
            List of coordinates of centroids of provided geometries

        Raises
        ______
        TypeError
            An invalid crs or datatype is used
        """
        if isinstance(data, gpd.GeoDataFrame):
            geom_col = data.geometry.name
            if crs == 3035:
                coords = np.array(
                    data[geom_col].apply(lambda geom: (geom.centroid.x, geom.centroid.y)).to_list())
            elif crs == 4326:
                coords = np.array(
                    data[geom_col].apply(lambda geom: (geom.centroid.x * np.pi / 180,
                                                       geom.centroid.y * np.pi / 180)).to_list())
            else:
                raise TypeError(f"{crs} is an unavailable crs for this operation")
        elif isinstance(data, list):
            if crs == 3035:
                coords = np.array([(geom.centroid.x, geom.centroid.y) for geom in data])
            elif crs == 4326:
                coords = np.array(
                    [(geom.centroid.x * np.pi / 180, geom.centroid.y * np.pi / 180) for geom in data])
            else:
                raise TypeError(f"{crs} is an unavailable crs for this operation")
        else:
            raise TypeError(f"Type {type(data)} is invalid")

        return coords
