import os
import projectdata
from collections import namedtuple
# ################################################################################
# THIS FILE CONTAINS ALL GENERAL PROJECT CONFIGURATIONS AS CONSTANTS
# ################################################################################

DATA_PATH = projectdata.__path__[0]


## Postgres configuration
#POSTGRES_HOST = os.environ['POSTGRES_HOST']
#POSTGRES_PORT = os.environ['POSTGRES_PORT']
#POSTGRES_USER = os.environ['POSTGRES_USER']
#POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
#POSTGRES_DATABASE = os.environ['POSTGRES_DATABASE']
#


######################################################
### Set parameters  ##################################
######################################################
db_path = os.path.join(DATA_PATH, "pricing.db")
excel_path = os.path.join(DATA_PATH, "pricing_data.xlsx")
print(db_path)

## Set other parameters
table_name_list = ['MAPPING_PAR_REF_AIRPORT', 'MAPPING_PAR_REF_GROUND_HANDLER',
                   'PAR_AIRPORT', 'PAR_GROUND_HANDLER']

required_keys = ['amount', 'ground_handlers',
                 'airports', 'timestamp']

# The tables and name of the column for the query corresponding to a key in the input
# The collection of names is a namedtuple, goes into a dict

# Define namedtuple
TableNames = namedtuple('TableNames', 'table_a table_b column_name')

# Define the dict with the namedtuple TableNames as value
sql_parameter_tablename_dict = {}
sql_parameter_tablename_dict['airports'] = TableNames('MAPPING_PAR_REF_AIRPORT',
                            'PAR_AIRPORT',
                            'AIRPORT_ID')

sql_parameter_tablename_dict['ground_handlers'] = TableNames('MAPPING_PAR_REF_GROUND_HANDLER',
                            'PAR_GROUND_HANDLER',
                            'GROUND_HANDLER_ID')

# Return a pandas DataFrame as overview (perhaps, other data structure is more suited?)

base_rate = 1.E-3 # This probably should be in the database in its own table, with FROM, TO
