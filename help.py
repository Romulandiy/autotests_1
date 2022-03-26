import os
import shutil
import subprocess

from aquas.libs.logger import logger


class Helpers:

    @staticmethod
    def get_string_result_from_data_frame(data_frame):
        current_list = data_frame.values.tolist()
        result = ', '.join([str(i) for sub_list in current_list for i in sub_list])
        logger.info(f'Result: {result}')
        return result

    @staticmethod
    def run_load_command(command, log=True):
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        if log:
            log_list = []
            while True:
                line = process.stdout.readline()
                if not line:
                    break
                log_list.append(str(line.rstrip()))
                logger.info(line.rstrip())
            return log_list
        process.wait()

    @staticmethod
    def get_files_from_path(path):
        sql_files_list = []
        for filename in os.listdir(path):
            for i in os.listdir(path + filename):
                if i.endswith('.sql'):
                    sql_files_list.append(f'{path + filename}/{i}')
        return sql_files_list

    @staticmethod
    def delete_all_from_folder(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logger.info(e)

    @staticmethod
    def is_user_created_in_oracle(oracle, user_name, columns):
        oracle.run_query(f"ALTER SESSION SET \"_ORACLE_SCRIPT\"=true")
        test = oracle.select_all(table_name='ALL_USERS', columns=[columns], condition=f"WHERE USERNAME = '{user_name}'")
        return not test.empty
