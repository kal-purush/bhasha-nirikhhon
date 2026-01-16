import shapefile
import netCDF4 as nc
import sqlite3
import json
import os

# region configs
# TODO modify the configs if needed
db_path = 'output/mesh.gwfvisdb'
nc_db_file_path = 'data/mesh/Drainage_database/BowBanff_MESH_drainage_database.nc'
shape_file_path = 'data/mesh/Shape/bow_distributed.shp'

layers = [1, 2, 3]
nc_file_base_path = 'data/mesh/MESH_state'
nc_file_path_and_variable_name_pairs = [
    [f'{nc_file_base_path}/STGW_M_GRD.nc', 'STGW', 'Total water storage [mm]', 'mm', None],
    [f'{nc_file_base_path}/SNO_M_GRD.nc', 'SNO', 'Liquid water content of the snow [mm]', 'mm', None],
    [f'{nc_file_base_path}/LQWSSNO_M_GRD.nc', 'LQWSSNO', 'Liquid water content of the snow [mm]', 'mm', None],
    [f'{nc_file_base_path}/LQWSPND_M_GRD.nc', 'LQWSPND', 'Liquid water storage of ponded water [mm]', 'mm', None],
    [f'{nc_file_base_path}/LQWSSOL_M_IG$$layer$$_GRD.nc', 'LQWSSOL', 'Liquid water storage in the soil [mm]', 'mm', layers],
    [f'{nc_file_base_path}/FZWSSOL_M_IG$$layer$$_GRD.nc', 'FZWSSOL', 'Frozen water storage in the soil [mm]', 'mm', layers],
    [f'{nc_file_base_path}/LQWSCAN_M_GRD.nc', 'LQWSCAN', 'Liquid water interception in the canopy [mm]', 'mm', None],
    [f'{nc_file_base_path}/FZWSCAN_M_GRD.nc', 'FZWSCAN', 'Frozen water interception in the canopy [mm]', 'mm', None],
    [f'{nc_file_base_path}/STGGW_M_GRD.nc', 'STGGW', 'Groundwater zone storage [mm]', 'mm', None],
    [f'{nc_file_base_path}/RFF_M_GRD.nc', 'RFF', 'Total runoff [mm]', 'mm', None],
]
# endregion

# region getting ready
try:
    os.remove(db_path)
except:
    pass

db_connection = sqlite3.connect(db_path)
db_cursor = db_connection.cursor()

nc_file_path = nc_file_path_and_variable_name_pairs[0][0]

reader = shapefile.Reader(shape_file_path)
nc_db = nc.Dataset(nc_db_file_path)
dataset = nc.Dataset(nc_file_path)
# endregion

# region create info table
db_cursor.execute('''
    create table info (
        key varchar primary key,
        value text,
        label varchar
    )
''')
db_cursor.execute('''
    insert into info (key, value, label) values ('name', 'mesh', 'Name')
''')
db_cursor.execute('''
    insert into info (key, value, label) values ('description', 'something...', 'Description')
''')
# endregion

# region create location table
db_cursor.execute('''
    create table location (
        id integer primary key,
        geometry text,
        metadata text
    )
''')

ids = list(nc_db.variables['seg_id'][:])
location_ids = []
for shape_record in reader.shapeRecords():
    id = int(shape_record.record['COMID'])
    location_ids.append(id)
    geometry = shape_record.shape.__geo_interface__
    geometry_json = json.dumps(geometry, indent=2)
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    metadata = dict(zip(field_names, shape_record.record))
    metadata_json = json.dumps(metadata, indent=2)
    db_cursor.execute('''
        insert into location (id, geometry, metadata) values (?, ?, ?)
    ''', [id, geometry_json, metadata_json])
# endregion

# region create dimension table
db_cursor.execute('''
    create table dimension (
        id integer primary key,
        name varchar not null,
        size int not null,
        description text,
        value_labels text
    )
''')
dimension_id_and_name_pairs = []

time_size = dataset.dimensions['time'].size
db_cursor.execute('''
        insert into dimension (id, name, size, description, value_labels) values (?, ?, ?, ?, ?)
    ''', [0, 'time', time_size, None, None])
dimension_id_and_name_pairs.append([0, 'time'])

layer_size = len(layers)
db_cursor.execute('''
        insert into dimension (id, name, size, description, value_labels) values (?, ?, ?, ?, ?)
    ''', [1, 'layer', layer_size, None, None])
dimension_id_and_name_pairs.append([1, 'layer'])
# endregion

# region create variable and variable_dimension table
db_cursor.execute('''
    create table variable (
        id integer primary key,
        name varchar not null,
        unit varchar,
        description text
    )
''')
db_cursor.execute('''
    create table variable_dimension (
        variable integer not null,
        dimension integer not null,
        foreign key (variable) references variable (id),
        foreign key (dimension) references dimension (id),
        primary key (variable, dimension)
    )
''')
variable_id_and_detail_pairs = []
index = 0
for [nc_file_path, variable_name, description, unit, layers] in nc_file_path_and_variable_name_pairs:
    db_cursor.execute('''
        insert into variable (id, name, unit, description) values (?, ?, ?, ?)
    ''', [index, variable_name, unit, description])
    db_cursor.execute('''
        insert into variable_dimension (variable, dimension) values (?, ?)
    ''', [index, 0])
    if layers:
        db_cursor.execute('''
            insert into variable_dimension (variable, dimension) values (?, ?)
        ''', [index, 1])
    variable_id_and_detail_pairs.append([index, [nc_file_path, variable_name, description, unit, layers]])
    index += 1
# endregion

# region create value table
new_line_character = '\n'
dimension_column_names = list(
    map(lambda x: f'dimension_{x[0]}', dimension_id_and_name_pairs))
db_cursor.execute(f'''
    create table value (
        location integer not null,
        variable integar not null,
        {f' integar,{new_line_character}'.join(dimension_column_names)},
        value float,
        foreign key (variable) references variable (id),
        primary key (location, variable, {f', '.join(dimension_column_names)})
    )
''')

for [variable_id, [nc_file_path, variable_name, description, unit, layers]] in variable_id_and_detail_pairs:
    if layers:
        for layer in layers:
            dataset = nc.Dataset(nc_file_path.replace('$$layer$$', str(layer)))
            current_variable = dataset.variables[variable_name]
            for time in range(dataset.dimensions['time'].size):
                for location_id in location_ids:
                    location_index = ids.index(location_id)
                    value = float(current_variable[time][location_index][0])
                    db_cursor.execute(f'''
                        insert into value (location, variable, dimension_0, dimension_1, value) values (?, ?, ?, ?, ?)
                    ''', [location_id, variable_id, time, layer - 1, value])
    else:
        dataset = nc.Dataset(nc_file_path)
        current_variable = dataset.variables[variable_name]
        # for only one dimension (time)
        for time in range(dataset.dimensions['time'].size):
            for location_id in location_ids:
                location_index = ids.index(location_id)
                value = float(current_variable[time][location_index][0])
                db_cursor.execute(f'''
                    insert into value (location, variable, dimension_0, dimension_1, value) values (?, ?, ?, ?, ?)
                ''', [location_id, variable_id, time, None, value])
# endregion

db_connection.commit()