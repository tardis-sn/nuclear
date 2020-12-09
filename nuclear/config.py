import logging
import os
import shutil

import yaml
from astropy.config import get_config_dir

from nuclear import __path__ as NUCLEAR_PATH

NUCLEAR_PATH = NUCLEAR_PATH[0]
DEFAULT_CONFIG_PATH = os.path.join(NUCLEAR_PATH,
                                   'default_tardisnuclear_config.yml')
DEFAULT_DATA_DIR = os.path.join(os.path.expanduser('~'),
                                'Downloads', 'tardisnuclear')
logger = logging.getLogger(__name__)


def get_configuration():

    config_fpath = os.path.join(get_config_dir(), 'tardisnuclear_config.yml')
    if not os.path.exists(config_fpath):
        logger.warning(f"Configuration File {config_fpath} does not exist - "
                       f"creating new one from default")
        shutil.copy(DEFAULT_CONFIG_PATH, config_fpath)
    return yaml.load(open(config_fpath), Loader=yaml.FullLoader)


def get_data_dir():
    config = get_configuration()
    data_dir = config.get('data_dir', None)
    if data_dir is None:
        config_fpath = os.path.join(get_config_dir(), 'tardisnuclear_config.yml')
        logging.critical('\n{line_stars}\n\nTARDISNUCLEAR will download nuclear data to its data directory {default_data_dir}\n\n'
                         'TARDISNUCLEAR DATA DIRECTORY not specified in {config_file}:\n\n'
                         'ASSUMING DEFAULT DATA DIRECTORY {default_data_dir}\n '
                         'YOU CAN CHANGE THIS AT ANY TIME IN {config_file} \n\n'
                         '{line_stars} \n\n'.format(line_stars='*'*80, config_file=config_fpath,
                                                     default_data_dir=DEFAULT_DATA_DIR))
        if not os.path.exists(DEFAULT_DATA_DIR):
            os.makedirs(DEFAULT_DATA_DIR)
        config['data_dir'] = DEFAULT_DATA_DIR
        yaml.dump(config, open(config_fpath, 'w'), default_flow_style=False)
        data_dir = DEFAULT_DATA_DIR

    if not os.path.exists(data_dir):
        raise IOError(f'Data directory specified in {data_dir} does not exist')
    logger.info(f"Using TARDISNuclear Data directory {data_dir}")
    return data_dir
