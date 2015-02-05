
def append_all(passed_list):
    'returns dataframe of all the lists passed put together'
    great_list = passed_list[0]
    for item, idx in zip(passed_list, range(1, len(passed_list))):
        great_list = great_list.append(passed_list[idx])
    great_list.reset_index(drop = True, inplace = True)
    return great_list

def check_data(passed_list, columns_list):
    'checks if the passed list has the same columns as the passed columns list'
    for item, idx in zip(passed_list, range(0,len(passed_list))):
        if (list(item.columns) == columns_list) is False:
            raise print('index: %s \ncolumn values no good bro' % idx)
        else:
            continue
            

def process(state, working_directory, name):
    
    #postgres doesn't like anything other than all lower case
    name = str.lower(name)
    
    #remove null lat/longs for geocoding 
    null_lat = state[state.latitude.isnull()]
    null_lon = state[state.longitude.isnull()]
    zero_lat = state[(state.latitude == 0)]
    zero_lon = state[(state.longitude == 0)]
    nulls = null_lat.append(null_lon)
    nulls = nulls.append(zero_lat)
    nulls = nulls.append(zero_lon)
    nulls['index'] = nulls.index
    nulls['index'].drop_duplicates(inplace = True)
    
    nulls.to_csv('{}_geocode_me/{}.csv'.format(working_directory, name))
    
    #select nonnull rows & format lat/lon
    state['index'] = state.index
    state = state[(~state['index'].isin(nulls['index']))]
    state[['latitude', 'longitude']] = state[['latitude', 'longitude']].convert_objects(convert_numeric=True)
    state['longitude'] = state.longitude.abs() * -1
    
    #create a list of states in passed list
    state_list = state['state'].value_counts().index.tolist()
    
    #postgres processing
    
    cur.execute("drop table if exists %s;" % name)
    conn.commit()
    
    state.to_sql('%s' % name, engine, chunksize=1000)
    
    cur.execute("ALTER TABLE %s ADD COLUMN geom geometry(POINT,4269);" % name)
    conn.commit()
    
    cur.execute("UPDATE %s SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4269);" % name)
    conn.commit()
    
    cur.execute("alter table %s add column in_state boolean;" % name)
    conn.commit()
    
    #for item in state_list:::::
    for item in state_list:
        cur.execute("update %s set in_state = st_intersects(%s.geom, (select state.geom from state where state.name = '%s')) where %s.state = '%s';" % (name, name, item, name, item))
        conn.commit()
        
def more_code():
	"add lines!, see if this is a commit to git"
    
    
    os.system("""pgsql2shp -f "{}_shapes/{}.shp" -h localhost -u ntapia -P postgres census "SELECT * FROM {} WHERE in_state = True" """.format(working_directory, name, name))
    
    cur.execute("COPY (SELECT * FROM {} WHERE in_state = False) TO '{}_munge_me/{}.csv' WITH CSV HEADER".format(name, working_directory, name))
    conn.commit()
    
    cur.execute("drop table if exists %s;" % name)
    conn.commit()
    

