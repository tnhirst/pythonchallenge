from pebble import ProcessPool
import tqdm
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon, shape
from sqlalchemy import create_engine, MetaData, Table, Column, String
from geoalchemy2 import Geometry
import psycopg2
from psycopg2.extras import execute_values
import fiona
import logging
import os

logger = logging.getLogger(__name__)


def _make_sub_grid_obj(sub_grid_dict):
    with fiona.open(sub_grid_dict['grid_path']) as coll:
        in_valid_region = coll.items(bbox=sub_grid_dict['valid_region'])
        for row in in_valid_region:
            nuts_0_id = row[1]['properties']['NUTS_0_ID'].split("-")[0]
            nuts_1_id = row[1]['properties']['NUTS_1_ID'].split("-")[0]
            nuts_2_id = row[1]['properties']['NUTS_2_ID'].split("-")[0]
            nuts_3_id = row[1]['properties']['NUTS_3_ID'].split("-")[0]
            if not sub_grid_dict['valid_nuts'] or \
                    any(n in [nuts_0_id, nuts_1_id, nuts_2_id, nuts_3_id] for n in sub_grid_dict['valid_nuts']):
                return GridDataset().prepare_subgrid(sub_grid_dict)
        return None


class GridDataset:

    def __init__(self):
        super().__init__()
        self.name = "grid"
        self.sub_grids = None
        self.datasets = [self]
        self.catchments = []
        self.valid_filter = None
        self.data_valid = None
        self.valid_region = None
        self.full_extent = None
        self.valid_nuts = None
        self.input_file = None
        self.complete = False
        self.attempts = None
        self.error = None

    def prepare(self, config):
        super().prepare(config)
        self.valid_nuts = config.get('valid_nuts')
        self.temp_file = os.path.join(self.temp_data_dir, 'grid_clean.gpkg')

        return self

    def prepare_subgrid(self, sub_grid_dict):
        self.valid_region = sub_grid_dict['valid_region']
        self.full_extent = sub_grid_dict['full_extent']
        self.valid_nuts = sub_grid_dict['valid_nuts']
        self.input_file = sub_grid_dict['grid_path']
        self.complete = False
        self.attempts = 0
        self.error = None

        return self

    def calculate_attributes(self):
        for catchment in self.catchments:
            if catchment.atts_to_agg:
                self.compute_for_catchment(catchment)

    def load_file(self):
        self.data = fiona.open(self.input_file)

        return self

    def preprocess(self):
        self.make_att_table()

        return self

    def make_att_table(self):
        logger.info("Creating attributes table in database")
        engine = create_engine(
            f"postgresql://{self.db_config['username']}:{self.db_config['password']}@{self.db_config['host']}:"
            f"{self.db_config['port']}/{self.db_config['database']}")

        if not engine.dialect.has_table(engine, 'attributes'):
            with create_engine(
                    f"postgresql://{self.db_config['username']}:{self.db_config['password']}@{self.db_config['host']}:"
                    f"{self.db_config['port']}/{self.db_config['database']}").connect() as conn, conn.begin():
                metadata = MetaData()
                att_table = Table('attributes', metadata,
                                  Column('grd_id', String, primary_key=True),
                                  Column('cntr_id', String),
                                  Column('nuts_0_id', String),
                                  Column('nuts_1_id', String),
                                  Column('nuts_2_id', String),
                                  Column('nuts_3_id', String),
                                  Column('geom', Geometry(geometry_type='MULTIPOLYGON', srid=0)))
                metadata.bind = conn
                att_table.create()

            rows = []
            for row in tqdm.tqdm(self.data):
                cntr_id = row['properties']['CNTR_ID'].split("-")[0]
                nuts_0_id = row['properties']['NUTS_0_ID'].split("-")[0]
                nuts_1_id = row['properties']['NUTS_1_ID'].split("-")[0]
                nuts_2_id = row['properties']['NUTS_2_ID'].split("-")[0]
                nuts_3_id = row['properties']['NUTS_3_ID'].split("-")[0]
                if not self.valid_nuts or \
                        any(n in [nuts_0_id, nuts_1_id, nuts_2_id, nuts_3_id] for n in self.valid_nuts):
                    geom = shape(row['geometry'])
                    if type(geom) == Polygon:
                        geom = MultiPolygon([geom])
                    rows.append((row['properties']['GRD_ID'], cntr_id, nuts_0_id, nuts_1_id, nuts_2_id, nuts_3_id, geom.wkb_hex))

            with psycopg2.connect(
                    user=self.db_config["username"],
                    password=self.db_config["password"],
                    host=self.db_config["host"],
                    port=self.db_config["port"],
                    database=self.db_config["database"]) as conn, conn.cursor() as cur:
                execute_values(cur, """INSERT INTO public.attributes (grd_id, cntr_id, 
                nuts_0_id, nuts_1_id, nuts_2_id, nuts_3_id, geom) VALUES %s""",
                               rows, page_size=10000)
                cur.execute("SELECT UpdateGeometrySRID('attributes', 'geom', 3035);")
                conn.commit()

    def apply_to(self, target):
        """Ensures that if a grid is attempted to be applied to another grid, it simply skips."""
        pass

    def compute_for_catchment(self, catchment):
        self.data.set_geometry('centroid', inplace=True)
        catchments = self.data[self.valid_filter]. \
            set_geometry(catchment.catchment_name)[['GRD_ID', catchment.catchment_name]]

        # For any cells for which the catchment is a point, make the cell a circle of diameter 1 such that the agg
        # only includes the cell in question
        catchments.loc[catchments.geom_type == 'Point', catchment.catchment_name] = catchments[catchments.geom_type == 'Point'].buffer(1)

        if self.full_extent is not None:
            if not catchments.envelope.within(Polygon([[self.full_extent[0], self.full_extent[1]],
                                                       [self.full_extent[2], self.full_extent[1]],
                                                       [self.full_extent[2], self.full_extent[3]],
                                                       [self.full_extent[0], self.full_extent[3]],
                                                       [self.full_extent[0], self.full_extent[1]]])).all():
                logger.warning("Some catchments extend beyond buffer.")

        aggregations = {c: [] for c in set(catchment.cols)}
        i = 0
        while i < len(catchment.cols):
            aggregations[catchment.cols[i]].append(catchment.aggs[i])
            i += 1

        r = gpd \
            .sjoin(catchments, self.data[catchment.cols + ['centroid']], how='inner', op='contains') \
            .groupby("GRD_ID") \
            .agg(aggregations) \
            .reset_index()
        r.columns = r.columns.droplevel(1)
        r.rename(columns=dict(zip(catchment.cols, catchment.atts_to_agg)), inplace=True)
        self.data.drop(catchment.atts_to_agg, axis=1, errors='ignore', inplace=True)
        self.data = self.data.merge(r, how='left', on='GRD_ID')

    def make_gdf_from_gpkg(self):
        logger.debug(f"Creating dataframe with bounds: {self.valid_region}")
        with fiona.open(self.input_file, bbox=self.full_extent) as coll:
            in_full_extent = coll.items(bbox=self.full_extent)
            data = []
            for row in in_full_extent:
                data.append({"GRD_ID": row[1]['properties']['GRD_ID'],
                             "CNTR_ID": row[1]['properties']['CNTR_ID'].split("-")[0],
                             "NUTS_0_ID": row[1]['properties']['NUTS_0_ID'].split("-")[0],
                             "NUTS_1_ID": row[1]['properties']['NUTS_1_ID'].split("-")[0],
                             "NUTS_2_ID": row[1]['properties']['NUTS_2_ID'].split("-")[0],
                             "NUTS_3_ID": row[1]['properties']['NUTS_3_ID'].split("-")[0],
                             "X_LLC": row[1]['properties']['X_LLC'],
                             "Y_LLC": row[1]['properties']['Y_LLC'],
                             "TOT_P_2011": row[1]['properties']['TOT_P_2011'],
                             "geometry": shape(row[1]['geometry'])})
        self.data = gpd.GeoDataFrame(data, crs=3035)
        self.data['centroid'] = self.data.centroid
        if self.valid_nuts:
            self.valid_filter = ((self.data['NUTS_0_ID'].isin([n for n in self.valid_nuts if len(n) == 2])) |
                                 (self.data['NUTS_1_ID'].isin([n for n in self.valid_nuts if len(n) == 3])) |
                                 (self.data['NUTS_2_ID'].isin([n for n in self.valid_nuts if len(n) == 4])) |
                                 (self.data['NUTS_3_ID'].isin([n for n in self.valid_nuts if len(n) == 5]))) & \
                                self.data.intersects(Polygon([[self.valid_region[0], self.valid_region[1]],
                                                              [self.valid_region[2], self.valid_region[1]],
                                                              [self.valid_region[2], self.valid_region[3]],
                                                              [self.valid_region[0], self.valid_region[3]],
                                                              [self.valid_region[0], self.valid_region[1]]]))
        else:
            self.valid_filter = self.data.intersects(Polygon([[self.valid_region[0], self.valid_region[1]],
                                                              [self.valid_region[2], self.valid_region[1]],
                                                              [self.valid_region[2], self.valid_region[3]],
                                                              [self.valid_region[0], self.valid_region[3]],
                                                              [self.valid_region[0], self.valid_region[1]]]))

    def clear_temp_files(self):
        os.remove(self.data_file)
        del self.data_file

    def split(self, split_size, buffer_size, cores=1, sub_grid_filter=None, cell_size=1000):
        logger.debug(f"Splitting grid with {cores} cores")
        bounds = self.data.bounds
        sub_grids = []
        x = bounds[0]
        y = bounds[1]
        while x < bounds[2]:
            while y < bounds[3]:
                sub_grids.append(
                    {"valid_region":
                         (x + cell_size / 2, y + cell_size / 2, x + split_size - cell_size / 2,
                          y + split_size - cell_size / 2),
                     "full_extent":
                         (x - buffer_size + cell_size / 2, y - buffer_size + cell_size / 2,
                          x + split_size + buffer_size - cell_size / 2, y + split_size + buffer_size - cell_size / 2),
                     "valid_nuts": self.valid_nuts,
                     "grid_path": self.input_file}
                )
                y += split_size
            x += split_size
            y = bounds[1]
        del self.data
        del self.data_valid
        if sub_grid_filter is not None:
            logger.debug("Filtering sub_grids")
            sub_grids = [s for s in sub_grids if sub_grid_filter(s)]
            logger.debug(sub_grids)
        splits = []
        if cores == 1:
            for sub_grid in tqdm.tqdm(sub_grids):
                split = _make_sub_grid_obj(sub_grid)
                if split:
                    splits.append(split)
        else:
            with tqdm.tqdm(total=len(sub_grids)) as pbar, ProcessPool(max_workers=cores) as pool:
                future = pool.map(_make_sub_grid_obj, sub_grids)
                iterator = future.result()
                while True:
                    try:
                        split = next(iterator)
                        if split:
                            splits.append(split)
                        pbar.update(1)
                    except StopIteration:
                        break
        return splits

    def gdf_as_csv(self, gdf, columns, valid_region=None):
        return GridCSVIterator(gdf, columns, valid_region)

    def fiona_as_csv(self, collection, filter):
        return FionaCSVIterator(collection, filter)
