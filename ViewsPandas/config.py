import os
import pwd
from dotenv import load_dotenv, find_dotenv

# Really primitive POC stuff, relying on envrionment vars
# Rewrite to a smarter system when discussed with Peder on overall architectural choice.
# Maybe fold into viewser?

ENV_LOCATIONS = ['~/.ingester3/ingester3.env',
                 '~/ingester.env',
                 '/etc/ingester3.env',
                 '/usr/local/etc/ingester3.env']

for location in ENV_LOCATIONS:
    location = os.path.expanduser(location)
    if find_dotenv(location) != '':
        load_dotenv(location)
        break


testing = os.getenv("INGESTER_TEST", 'False').lower() in ('true', '1', 't')

working_dir = '~/.ingester3' if os.getenv('INGESTER_DIR') is None else os.getenv('INGESTER_DIR').lower()
working_dir = os.path.expanduser(working_dir)
os.makedirs(os.path.dirname(working_dir), exist_ok=True)


views_user = pwd.getpwuid(os.getuid()).pw_name if os.getenv('INGESTER_USER') is None else os.getenv('INGESTER_USER').lower()
views_host = 'localhost' if os.getenv('INGESTER_HOST') is None else os.getenv('INGESTER_HOST').lower()
views_db = 'fallback3_test' if testing else 'fallback3'

source_db_path = f'postgresql://{views_user}@{views_host}:5432/{views_db}'
source_db_path = source_db_path if os.getenv('INGESTER_URI') is None else os.getenv('INGESTER_URI')

source_cache_path = os.path.join(os.path.expanduser(working_dir), 'db_cache')
inner_cache_path = os.path.join(os.path.expanduser(working_dir), 'inner_cache')
log_file = os.path.join(os.path.expanduser(working_dir), 'log.log')

log_level = 'DEBUG' if os.getenv('INGESTER_LOGGING') is None else os.getenv('INGESTER_LOGGING')