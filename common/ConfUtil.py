import os
from common.ConfigParser import data
from common.Logger import logger

BASE_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def get_data(yaml_file_name):
    try:
        data_file_path = os.path.join(BASE_PATH, "resource", yaml_file_name)
        yaml_data = data.load_yaml(data_file_path)
    except Exception as ex:
        logger.info(ex)
    else:
        return yaml_data
