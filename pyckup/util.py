"""Utils for pyckyp"""

import os
from importlib.resources import files
from functools import partial

from config2py import simple_config_getter, get_app_data_folder, process_path


pkg_name = 'pyckup'

# ---------------- PKG DATA -----------------------------------------------------

get_config = simple_config_getter(pkg_name)

data_files = files(pkg_name) / "data"
app_data_dir = os.environ.get(
    f"{pkg_name.upper()}_APP_DATA_DIR", get_app_data_folder(pkg_name)
)

# ---------------- LOCAL DATA -----------------------------------------------------
app_data_dir = process_path(app_data_dir, ensure_dir_exists=True)
djoin = partial(os.path.join, app_data_dir)

downloads_dir = process_path(djoin("downloads"), ensure_dir_exists=True)
