import os
import pytest

from aquas.libs.test_base import TestBase
from aquas.plugins.hive_plugin import HivePlugin
from aquas.plugins.oracle_plugin import OraclePlugin

from autotests.test.tests.helpers import Helpers


# schema
USER_NAME = 'CUST'

# vars from test_config.yaml
FILE_DIR = os.path.dirname(__file__)
CONFIG_PATH_DEFAULT = os.path.join(FILE_DIR, 'resources/configs/test_config.yaml')
CONFIG = TestBase.load_yaml_file(CONFIG_PATH_DEFAULT)
DATA_CONF = CONFIG['data']
ORACLE_CONF = CONFIG['oracle']
HIVE_CONF = CONFIG['hive']
PATH_CONF = CONFIG['paths']
KEYTAB = os.path.join(FILE_DIR, PATH_CONF['keytab'])
LOCAL_INBOX = PATH_CONF['local_inbox']

# .sql script paths
SQL_DDL_SCRIPTS_PATH_DEFAULT = os.path.join(FILE_DIR, 'resources/sql_scripts/create')
SQL_DML_SCRIPTS_PATH_DEFAULT_IN_TEST_PREPARATION = os.path.join(FILE_DIR, 'resources/sql_scripts/insert_in_test_preparation')


# Clears everything before the test if there is something. Then creates new test data.
@pytest.fixture(scope='session', autouse=True)
def test_preparation(hive, oracle):
    Helpers.delete_all_from_folder(LOCAL_INBOX)

    hive.run_query('USE RAW')

    for table, view in zip(DATA_CONF['hive_table_names'], DATA_CONF['hive_view_names']):
        hive.drop_table(table)
        hive.drop_view(view)

    if Helpers.is_user_created_in_oracle(oracle, USER_NAME, columns='USERNAME'):
        oracle.drop_user(USER_NAME)
    oracle.create_user(USER_NAME)

    for sql_file in DATA_CONF['create_sql_files']:
        sql_create_script = oracle.read_from_sql_file(os.path.join(SQL_DDL_SCRIPTS_PATH_DEFAULT, sql_file))
        oracle.run_query(sql_create_script)

    for sql_file in DATA_CONF['insert_in_test_preparation_sql_files']:
        oracle.execute_all_from_file_by_line(os.path.join(SQL_DML_SCRIPTS_PATH_DEFAULT_IN_TEST_PREPARATION, sql_file))


@pytest.fixture(scope='session')
def hive():
    with HivePlugin(host=HIVE_CONF['host'],
                    port=HIVE_CONF['port'],
                    user=HIVE_CONF['user'],
                    database=HIVE_CONF['database'],
                    kerberos=True,
                    keytab=KEYTAB) as hive_instance:
        yield hive_instance


@pytest.fixture(scope='session')
def oracle():
    with OraclePlugin(user=ORACLE_CONF['user'],
                      password=ORACLE_CONF['password'],
                      port=ORACLE_CONF['port']) as oracle_instance:
        yield oracle_instance
