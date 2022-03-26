import datetime
import os
import numpy as np

from multiprocessing import Pool

from aquas.libs.logger import logger
from aquas.libs.test_base import TestBase
from aquas.plugins.oracle_plugin import OraclePlugin

from autotests.foris.tests.helpers import Helpers


FILE_DIR = os.path.dirname(__file__)
CONFIG_PATH_DEFAULT = os.path.join(FILE_DIR, 'resources/configs/test_config.yaml')
CONFIG = TestBase.load_yaml_file(CONFIG_PATH_DEFAULT)

# .sh and .sql script paths
SQL_DDL_SCRIPTS_PATH_DEFAULT = os.path.join(FILE_DIR, 'resources/sql_scripts/create')
SQL_DML_SCRIPTS_PATH_DEFAULT_IN_TEST_PREPARATION = os.path.join(FILE_DIR, 'resources/sql_scripts/insert_in_test_preparation')
SQL_DML_SCRIPTS_PATH_DEFAULT_IN_TESTS = os.path.join(FILE_DIR, 'resources/sql_scripts/insert_in_tests')
PREPARE_SH_SCRIPT = os.path.join(FILE_DIR, 'resources/sh_scripts/prepare.sh')

# config paths
CONFIG_SHAPSHOT = os.path.join(FILE_DIR, 'resources/configs/config_shapshot.yaml')
CONFIG_INC_DATE = os.path.join(FILE_DIR, 'resources/configs/config_inc_date.yaml')
CONFIG_INC_ID_1 = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_1.yaml')
CONFIG_INC_ID_2 = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_2.yaml')
CONFIG_INC_ID_DATE = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_date.yaml')
CONFIG_INC_ID_DATE_SOURCE_MULTI_THREAD_1 = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_date_source_multi_thread_1.yaml')
CONFIG_INC_ID_BELOW_HWM_DATE = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_below_hwm_date.yaml')
CONFIG_INC_ID_HIGHER_HWM_DATE = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_higher_hwm_date.yaml')
CONFIG_FIVE_DAYS_SHAPSHOT = os.path.join(FILE_DIR, 'resources/configs/config_five_days_shapshot.yaml')
CONFIG_INC_ID_WITH_WAIT_FOR_SIXTY_DATE = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_with_wait_for_sixty_date.yaml')
CONFIG_INC_ID_WITH_SKIP_GAP = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_with_skip_gap.yaml')
CONFIG_INC_ID_WITH_FETCH_NEXT = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_with_fetch_next.yaml')
CONFIG_INC_ID_WITH_THREAD_NUM = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_with_thread_num.yaml')
CONFIG_INC_ID_DATE_PROBABLY_1 = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_date_probably_1.yaml')
CONFIG_INC_ID_DATE_PROBABLY_2 = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_date_probably_2.yaml')
CONFIG_INC_ID_DATE_PARTITION_IS_CROSSED_1 = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_date_partition_is_crossed_1.yaml')
CONFIG_INC_ID_DATE_PARTITION_IS_CROSSED_2 = os.path.join(FILE_DIR, 'resources/configs/config_inc_id_date_partition_is_crossed_2.yaml')

# schema
USER_NAME = 'CUST'

# vars from test_config.yaml
DATA_CONF = CONFIG['data']
PATH_CONF = CONFIG['paths']
LOCAL_INBOX = PATH_CONF['local_inbox']

# oracle table names
SNAPSHOT_TABLE = DATA_CONF['oracle_table_names'][0]  # QA_snapshot
INC_ID_TABLE = DATA_CONF['oracle_table_names'][1]  # QA_inc_id
INC_ID_WITH_SKIP_GAP_TABLE = DATA_CONF['oracle_table_names'][2]  # QA_inc_id_with_skip_gap
INC_ID_WITH_FETCH_NEXT_TABLE = DATA_CONF['oracle_table_names'][3]  # QA_inc_id_with_fetch_next
INC_ID_WITH_THREAD_NUM_TABLE = DATA_CONF['oracle_table_names'][4]  # QA_inc_id_with_thread_num
INC_ID_DATE_SOURCE_MULTI_THREAD_1 = DATA_CONF['oracle_table_names'][5]  # QA_inc_id_date_source_multi_thread_1

# hive table names
HIVE_SNAPSHOT_TABLE = DATA_CONF['hive_table_names'][0]  # mtsru_frs_kbn_orclcdb__cust_qa_snapshot_orig
HIVE_INC_ID_TABLE = DATA_CONF['hive_table_names'][1]  # mtsru_frs_kbn_orclcdb__cust_qa_inc_id_orig
HIVE_INC_DATE_TABLE = DATA_CONF['hive_table_names'][2]  # mtsru_frs_kbn_orclcdb__cust_qa_inc_date_orig
HIVE_INC_ID_DATE_TABLE = DATA_CONF['hive_table_names'][3]  # mtsru_frs_kbn_orclcdb__cust_qa_inc_id_date_orig
HIVE_INC_ID_BELOW_HWM_DATE_TABLE = DATA_CONF['hive_table_names'][4]  # mtsru_frs_kbn_orclcdb__cust_qa_inc_id_below_hwm_date_orig
HIVE_INC_ID_HIGHER_HWM_DATE_TABLE = DATA_CONF['hive_table_names'][5]  # mtsru_frs_kbn_orclcdb__cust_qa_inc_id_higher_hwm_date_orig
HIVE_FIVE_DAYS_SNAPSHOT_TABLE = DATA_CONF['hive_table_names'][6]  # mtsru_frs_kbn_orclcdb__cust_qa_five_days_snapshot_orig
HIVE_INC_ID_WITH_WAIT_FOR_SIXTY_DATE_TABLE = DATA_CONF['hive_table_names'][7]  # mtsru_frs_kbn_orclcdb__cust_qa_inc_id_with_wait_for_sixty_date_orig
HIVE_INC_ID_WITH_SKIP_GAP_TABLE = DATA_CONF['hive_table_names'][8]  # mtsru_frs_kbn_orclcdb__cust_qa_inc_id_with_skip_gap_orig
HIVE_INC_ID_WITH_FETCH_NEXT_TABLE = DATA_CONF['hive_table_names'][9]  # mtsru_frs_kbn_orclcdb__cust_qa_inc_id_with_fetch_next_orig
HIVE_INC_ID_WITH_THREAD_NUM_TABLE = DATA_CONF['hive_table_names'][10]  # mtsru_frs_kbn_orclcdb__cust_qa_inc_id_with_thread_num_orig
HIVE_INC_ID_DATE_PROBABLY_1_TABLE = DATA_CONF['hive_table_names'][11]  # mtsru_frs_kbn_orclcdb__cust_qa_inc_id_date_probably_1_orig
HIVE_INC_ID_DATE_SOURCE_MULTI_THREAD_1_TABLE = DATA_CONF['hive_table_names'][12] # mtsru_frs_kbn_orclcdb__cust_qa_inc_id_date_source_multi_thread_1_orig

# insert in tests
INSERT_INC_ID_2 = DATA_CONF['insert_in_tests_sql_files'][0]
INSERT_INC_DATE_2 = DATA_CONF['insert_in_tests_sql_files'][1]

# bunch oracle table name with user name
SNAPSHOT_TABLE_NAME = f'{USER_NAME}.{SNAPSHOT_TABLE}'
INC_ID_TABLE_NAME = f'{USER_NAME}.{INC_ID_TABLE}'
INC_ID_WITH_SKIP_GAP_TABLE_NAME = f'{USER_NAME}.{INC_ID_WITH_SKIP_GAP_TABLE}'
INC_ID_WITH_FETCH_NEXT_TABLE_NAME = f'{USER_NAME}.{INC_ID_WITH_FETCH_NEXT_TABLE}'
INC_ID_WITH_THREAD_NUM_TABLE_NAME = f'{USER_NAME}.{INC_ID_WITH_THREAD_NUM_TABLE}'
INC_ID_DATE_SOURCE_MULTI_THREAD_1_TABLE_NAME = f'{USER_NAME}.{INC_ID_DATE_SOURCE_MULTI_THREAD_1}'

# argument for run_load_command
COMMAND_PREPARE_SH_SCRIPT_1 = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_SHAPSHOT)
COMMAND_PREPARE_SH_SCRIPT_2 = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_DATE_PROBABLY_1)
COMMAND_PREPARE_SH_SCRIPT_3 = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_DATE_PROBABLY_2)
COMMAND_PREPARE_SH_SCRIPT_4 = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_DATE)
COMMAND_PREPARE_SH_SCRIPT_5 = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_1)
COMMAND_PREPARE_SH_SCRIPT_6 = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_DATE)
COMMAND_PREPARE_SH_SCRIPT_7 = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_2)
COMMAND_PREPARE_SH_SCRIPT_HIGHER_HWM_DATE = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_HIGHER_HWM_DATE)
COMMAND_PREPARE_SH_SCRIPT_PARTITION_IS_CROSSED_1 = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_DATE_PARTITION_IS_CROSSED_1)
COMMAND_PREPARE_SH_SCRIPT_PARTITION_IS_CROSSED_2 = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_DATE_PARTITION_IS_CROSSED_2)
COMMAND_PREPARE_SH_SCRIPT_BELOW_HWM_DATE = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_BELOW_HWM_DATE)
COMMAND_PREPARE_SH_SCRIPT_WITH_FOR_SIXTY_DATE = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_WITH_WAIT_FOR_SIXTY_DATE)
COMMAND_PREPARE_SH_SCRIPT_WITH_SKIP_GAP = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_WITH_SKIP_GAP)
COMMAND_PREPARE_SH_SCRIPT_WITH_FETCH_NEXT = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_WITH_FETCH_NEXT)
COMMAND_PREPARE_SH_SCRIPT_WITH_THREAD_NUM = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_WITH_THREAD_NUM)
COMMAND_PREPARE_SH_SCRIPT_SOURCE_MULTI_THREAD_1 = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader4.yaml',
    yaml_config=CONFIG_INC_ID_DATE_SOURCE_MULTI_THREAD_1)
COMMAND_PREPARE_SH_SCRIPT_WITH_FIX_GAP = 'sh {prepare_sh_script} {frs_conf} {yaml_config} {fix_gap}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_INC_ID_DATE_PROBABLY_1,
    fix_gap='-fix_gap 1')
COMMAND_PREPARE_SH_SCRIPT_NOT_CREATE_DDL = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader2.yaml',
    yaml_config=CONFIG_SHAPSHOT)
COMMAND_PREPARE_SH_SCRIPT_FIVE_DAYS = 'sh {prepare_sh_script} {frs_conf} {yaml_config}'.format(
    prepare_sh_script=PREPARE_SH_SCRIPT,
    frs_conf='foris-loader.yaml',
    yaml_config=CONFIG_FIVE_DAYS_SHAPSHOT)


class TestCoreDataLakeForis:

    def test_check_connection_and_loaded_data_of_table(self, hive, oracle):
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_1, True)

        columns = ['BANK_ID', 'BANK_NAME', 'DATE_OF_CHANGE']
        df_from_oracle = oracle.select_all(table_name=SNAPSHOT_TABLE_NAME, columns=columns)

        df_from_hive = hive.select_all(table_name=HIVE_SNAPSHOT_TABLE, columns=columns, drop_table_names=True)

        expected_result = Helpers.get_string_result_from_data_frame(df_from_oracle)
        actual_result = Helpers.get_string_result_from_data_frame(df_from_hive)
        assert expected_result == actual_result, 'Data in checked fields did not match'

    def test_check_source_multi_thread_1(self, oracle, hive):
        df_from_oracle = oracle.select_all(table_name=INC_ID_DATE_SOURCE_MULTI_THREAD_1_TABLE_NAME)
        expected_rows = len(df_from_oracle)

        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_SOURCE_MULTI_THREAD_1, True)

        df_from_hive = hive.select_all(table_name=HIVE_INC_ID_DATE_SOURCE_MULTI_THREAD_1_TABLE,
                                       drop_table_names=True)
        actual_rows = len(df_from_hive)

        assert expected_rows == actual_rows

    def test_check_loaded_data_for_five_days_by_condition(self, hive):
        expected_result = 5

        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_FIVE_DAYS, True)

        df_from_hive = hive.select_all(table_name=HIVE_FIVE_DAYS_SNAPSHOT_TABLE, drop_table_names=True)
        count_rows = len(df_from_hive)

        assert expected_result == count_rows

    def test_check_names_of_all_fields_in_oracle_and_hive_tables(self, oracle, hive):
        expected_names_of_tech_fields_in_hive_table = \
            {'raw_ts', 'batch_id', 'input_file_name', 'source_tz', 'raw_dt', 'version_dt'}
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_1, True)

        df_from_oracle = oracle.select_all(table_name=SNAPSHOT_TABLE_NAME)
        df_from_hive = hive.select_all(table_name=HIVE_SNAPSHOT_TABLE, drop_table_names=True)

        expected_names_of_oracle_fields = [x.lower() for x in df_from_oracle.keys().values]
        actual_result_names_of_hive_fields = [x.lower() for x in df_from_hive.keys().values[:-6]]

        assert expected_names_of_tech_fields_in_hive_table.issubset(df_from_hive), \
            'Tech fields in hive are missing or do not match'
        assert expected_names_of_oracle_fields == actual_result_names_of_hive_fields, \
            'Names of hive fields does not match with oracle fields'

    def test_check_loaded_data_of_inc_table_by_date(self, hive, oracle):
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_4, True)

        df_before_load = hive.select_all(table_name=HIVE_INC_DATE_TABLE, drop_table_names=True)
        count_rows_before = len(df_before_load)
        Helpers.delete_all_from_folder(LOCAL_INBOX)

        oracle.execute_all_from_file_by_line(os.path.join(SQL_DML_SCRIPTS_PATH_DEFAULT_IN_TESTS, INSERT_INC_DATE_2))

        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_4, True)

        df_after_load = hive.select_all(table_name=HIVE_INC_DATE_TABLE, drop_table_names=True)
        count_rows_after = len(df_after_load)

        assert count_rows_before < count_rows_after, 'In Hive count rows <= even after second download enriched Oracle table'

    def test_check_loaded_data_of_inc_table_by_id(self, hive, oracle):
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_5, True)

        df_from_oracle = oracle.select_all(table_name=INC_ID_TABLE_NAME)

        df_from_hive = hive.select_all(table_name=HIVE_INC_ID_TABLE, drop_table_names=True)

        oracle_count_rows = len(df_from_oracle)
        hive_count_rows = len(df_from_hive)

        assert oracle_count_rows == hive_count_rows

    def test_check_loaded_data_of_inc_table_by_id_with_skip_gap(self, hive, oracle):
        expected_result_sentence = 'select min'
        list_of_logs = Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_WITH_SKIP_GAP, True)

        df_from_oracle = oracle.select_all(table_name=INC_ID_WITH_SKIP_GAP_TABLE_NAME)

        df_from_hive = hive.select_all(table_name=HIVE_INC_ID_WITH_SKIP_GAP_TABLE, drop_table_names=True)

        oracle_count_rows = len(df_from_oracle)
        hive_count_rows = len(df_from_hive)

        assert any(expected_result_sentence in s for s in list_of_logs), \
            'There is no characteristic phrase "select mins" in the logs'
        assert oracle_count_rows == hive_count_rows, \
            'The count of rows oracle and hive does not match'

    def test_check_loaded_data_of_inc_table_by_id_with_fetch_next(self, hive, oracle):
        expected_result_sentence = 'rows fetch next'
        list_of_logs = Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_WITH_FETCH_NEXT, True)

        df_from_oracle = oracle.select_all(table_name=INC_ID_WITH_FETCH_NEXT_TABLE_NAME)

        df_from_hive = hive.select_all(table_name=HIVE_INC_ID_WITH_FETCH_NEXT_TABLE,
                                       drop_table_names=True)

        oracle_count_rows = len(df_from_oracle)
        hive_count_rows = len(df_from_hive)

        assert any(expected_result_sentence in s for s in list_of_logs), \
            'There is no characteristic phrase "rows fetch next" in the logs'
        assert oracle_count_rows == hive_count_rows, \
            'The count of rows oracle and hive does not match'

    def test_check_loaded_data_of_inc_table_by_id_with_thread_num(self, hive, oracle):
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_WITH_THREAD_NUM, True)

        df_from_oracle = oracle.select_all(table_name=INC_ID_WITH_THREAD_NUM_TABLE_NAME)

        df_from_hive = hive.select_all(table_name=HIVE_INC_ID_WITH_THREAD_NUM_TABLE,
                                       drop_table_names=True)

        oracle_count_rows = len(df_from_oracle)
        hive_count_rows = len(df_from_hive)

        assert oracle_count_rows == hive_count_rows

    def test_check_loaded_data_of_inc_table_by_id_if_change_partition_from_to(self, hive, oracle):
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_5, True)

        oracle.execute_all_from_file_by_line(os.path.join(SQL_DML_SCRIPTS_PATH_DEFAULT_IN_TESTS, INSERT_INC_ID_2))
        df_from_oracle = oracle.select_all(table_name=INC_ID_TABLE_NAME)

        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_7, True)

        df_from_hive = hive.select_all(table_name=HIVE_INC_ID_TABLE,
                                       drop_table_names=True)
        oracle_count_rows = len(df_from_oracle)
        hive_count_rows = len(df_from_hive)

        assert oracle_count_rows > hive_count_rows

    def test_check_loaded_data_of_inc_table_by_id_and_date(self, hive):
        now = datetime.datetime.now().strftime('%Y-%m-%d')
        expected_result = [
            (f'raw_dt={now}/part_from=32020000000/part_to=32023287037/business_dt=2020-01-15/parts=2',),
            (f'raw_dt={now}/part_from=32020000000/part_to=32023287037/business_dt=2020-01-16/parts=2',)
        ]

        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_6, True)

        hive.run_query('USE RAW')
        actual_result = hive.run_query(f'SHOW PARTITIONS {HIVE_INC_ID_DATE_TABLE}')

        assert expected_result == actual_result

    def test_check_loaded_data_of_inc_table_by_id_with_wait_for_sixty_and_date(self, hive):
        expected_result_sentence = 'Wait before load 10 sec'
        now = datetime.datetime.now().strftime('%Y-%m-%d')
        expected_result_show_partitions = [
            (f'raw_dt={now}/part_from=32023275183/part_to=32023286991/business_dt=2020-01-15/parts=2',),
            (f'raw_dt={now}/part_from=32023275183/part_to=32023286991/business_dt=2020-01-16/parts=2',)
        ]

        list_of_logs = Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_WITH_FOR_SIXTY_DATE, True)

        hive.run_query('USE RAW')
        actual_result = hive.run_query(f'SHOW PARTITIONS {HIVE_INC_ID_WITH_WAIT_FOR_SIXTY_DATE_TABLE}')

        assert any(expected_result_sentence in s for s in list_of_logs), \
            'There is no characteristic phrase "Wait before load 10 sec" in the logs'
        assert expected_result_show_partitions == actual_result, \
            'Partitions in hive does not match'

    def test_check_loaded_data_of_inc_table_by_id_below_hwm_and_date(self, hive):
        now = datetime.datetime.now().strftime('%Y-%m-%d')
        expected_result = [
            (f'raw_dt={now}/part_from=32023272745/part_to=32023274747/business_dt=2020-01-15/parts=1',)
        ]

        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_BELOW_HWM_DATE, True)

        hive.run_query('USE RAW')
        actual_result = hive.run_query(f'SHOW PARTITIONS {HIVE_INC_ID_BELOW_HWM_DATE_TABLE}')

        assert expected_result == actual_result, 'Downloaded hive partitions did not match with expected partitions'

    def test_check_loaded_data_of_inc_table_by_id_higher_hwm_and_date(self, hive):
        now = datetime.datetime.now().strftime('%Y-%m-%d')
        expected_result = [
            (f'raw_dt={now}/part_from=32023287035/part_to=32023287037/business_dt=2020-01-16/parts=1',)
        ]

        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_HIGHER_HWM_DATE, True)

        hive.run_query('USE RAW')
        actual_result = hive.run_query(f'SHOW PARTITIONS {HIVE_INC_ID_HIGHER_HWM_DATE_TABLE}')

        assert expected_result == actual_result, 'Downloaded hive partitions did not match with expected partitions'

    def test_check_generation_ddl_scripts(self):
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_1)

        expected_view = OraclePlugin.read_from_sql_file(os.path.join(SQL_DDL_SCRIPTS_PATH_DEFAULT, 'QA_snapshot_viewkbn_ORCLCDB.sql'))
        expected_table = OraclePlugin.read_from_sql_file(os.path.join(SQL_DDL_SCRIPTS_PATH_DEFAULT, 'QA_snapshot_tablekbn_ORCLCDB.sql'))

        filename = Helpers.get_files_from_path(LOCAL_INBOX)
        try:
            view_file = [s for s in filename if "QA_snapshot_viewkbn" in s][0]
            actual_view = OraclePlugin.read_from_sql_file(view_file)
        except IndexError:
            logger.info('DDL file for view not found')
            actual_view = ""
        try:
            table_file = [s for s in filename if "QA_snapshot_tablekbn" in s][0]
            actual_table = OraclePlugin.read_from_sql_file(table_file)
        except IndexError:
            logger.info('DDL file for table not found')
            actual_table = ""

        expected_result = expected_view + expected_table
        actual_result = actual_view + actual_table

        assert expected_result == actual_result, 'Expected view and table did not match with actual view and table'

    def test_check_none_does_not_become_zero_in_value_field(self, hive, oracle):
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_1, True)

        columns = ['SN', 'PARENT_BANK_ID', 'COMMISSION_PERCENT', 'DESCRIPTION']
        df_from_oracle = oracle.select_all(table_name=SNAPSHOT_TABLE_NAME, columns=columns)
        df_from_hive = hive.select_all(table_name=HIVE_SNAPSHOT_TABLE, columns=columns, drop_table_names=True)

        assert np.all(df_from_hive.values != 0), \
            'None transformationed in zero in value field'
        assert np.all(df_from_hive.values == df_from_oracle.values), \
            'Value of fields oracle and hive do not match'

    def test_check_generation_not_create_ddl_scripts(self):
        expected_result = 'Execute query: create table if not exists mtsru_frs_kbn_ORCLCDB__CUST_QA_snapshot_orig('
        list_of_logs = Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_NOT_CREATE_DDL, True)

        assert any(expected_result not in s for s in list_of_logs)

    def test_check_concurrent_loaded_data_of_single_table(self):
        pool = Pool(2)
        p = pool.starmap(Helpers.run_load_command, [(COMMAND_PREPARE_SH_SCRIPT_1, True),
                                                    (COMMAND_PREPARE_SH_SCRIPT_1, True)])

        assert 'Table mtsru_frs_kbn_ORCLCDB__CUST_QA_snapshot_orig is locked by user' in str(p)

    def test_check_message_partition_is_crossed(self):
        expected_result = 'Partition is crossed: hive partition [19, 36] ' \
                          'and candidate [20, 30]'
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_PARTITION_IS_CROSSED_1, True)

        list_of_logs = Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_PARTITION_IS_CROSSED_2, True)

        assert any(expected_result in s for s in list_of_logs)

    def test_check_message_probably_incompleted_partition(self, hive):
        expected_result = 'Incompleted partition for mtsru_frs_kbn_ORCLCDB__CUST_QA_inc_id_date_probably_1_orig: ' \
                          'part_from=1607846761/part_to=1607846765/parts=1 next is ' \
                          'part_from=1607846771/part_to=1607846771/parts=1, use parameter for repair -fix_gap 1'
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_2, True)

        hive.run_query('USE RAW')
        temp_result = hive.run_query(f'SHOW PARTITIONS {HIVE_INC_ID_DATE_PROBABLY_1_TABLE}')
        logger.info(f'temp_result = {temp_result}')

        hive.run_query(f'ALTER TABLE {HIVE_INC_ID_DATE_PROBABLY_1_TABLE} '
                       f'DROP PARTITION (part_from=1607846766)')

        list_of_logs = Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_3, True)
        temp_result_2 = hive.run_query(f'SHOW PARTITIONS {HIVE_INC_ID_DATE_PROBABLY_1_TABLE}')
        logger.info(f'temp_result_2 = {temp_result_2}')

        assert any(expected_result in s for s in list_of_logs)

    def test_check_key_fix_gap(self, hive):
        now = datetime.datetime.now().strftime('%Y-%m-%d')
        expected_result = [
            (f'raw_dt={now}/part_from=1607846761/part_to=1607846765/business_dt=2003-05-31/parts=1',),
            (f'raw_dt={now}/part_from=1607846766/part_to=1607846770/business_dt=2003-08-31/parts=2',),
            (f'raw_dt={now}/part_from=1607846766/part_to=1607846770/business_dt=2003-09-30/parts=2',),
            (f'raw_dt={now}/part_from=1607846771/part_to=1607846771/business_dt=2003-09-30/parts=1',),
            (f'raw_dt={now}/part_from=1607846772/part_to=1607846775/business_dt=2003-04-30/parts=3',),
            (f'raw_dt={now}/part_from=1607846772/part_to=1607846775/business_dt=2003-07-31/parts=3',),
            (f'raw_dt={now}/part_from=1607846772/part_to=1607846775/business_dt=2003-09-30/parts=3',),
            (f'raw_dt={now}/part_from=1607846776/part_to=1607846777/business_dt=2003-04-30/parts=2',),
            (f'raw_dt={now}/part_from=1607846776/part_to=1607846777/business_dt=2003-10-31/parts=2',)
        ]

        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_2, True)

        hive.run_query('USE RAW')
        temp_partitions = hive.run_query(f'SHOW PARTITIONS {HIVE_INC_ID_DATE_PROBABLY_1_TABLE}')
        logger.info(temp_partitions)
        hive.run_query(f'ALTER TABLE {HIVE_INC_ID_DATE_PROBABLY_1_TABLE} '
                       f'DROP PARTITION (part_from=1607846771, part_to=1607846771)')

        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_3, True)

        temp_partitions = hive.run_query(f'SHOW PARTITIONS {HIVE_INC_ID_DATE_PROBABLY_1_TABLE}')
        logger.info(temp_partitions)

        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_WITH_FIX_GAP, True)

        actual_result = hive.run_query(f'SHOW PARTITIONS {HIVE_INC_ID_DATE_PROBABLY_1_TABLE}')

        assert expected_result == actual_result, 'Partitions were not repaired with key -fix_gap 1'

    def test_check_loaded_data_of_table_if_added_new_column(self, oracle):
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_1)

        oracle.add_new_column(table_name=SNAPSHOT_TABLE_NAME,
                              column_name='TEST',
                              data_type='VARCHAR2(255)')

        logs = Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_1, True)

        assert any('Invalid table alias or column reference' in s for s in logs), 'Check of table structure failed'

    def test_check_loaded_data_of_table_if_delete_column(self, oracle):
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_1, True)

        oracle.delete_column(table_name=SNAPSHOT_TABLE_NAME, column_name='BANK_ID')

        logs = Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_1, True)

        assert any('Invalid table alias or column reference' in s for s in logs), 'Check of table structure failed'

    def test_check_loaded_data_of_table_if_swap_two_columns(self, oracle):
        Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_1, True)

        oracle.swap_columns(SNAPSHOT_TABLE_NAME, 'FILIAL_NUMBER', 'DESCRIPTION')

        logs = Helpers.run_load_command(COMMAND_PREPARE_SH_SCRIPT_1, True)

        assert any('Invalid table alias or column reference' in s for s in logs), 'Check of table structure failed'
