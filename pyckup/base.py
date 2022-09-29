"""
Base
"""
from contextlib import suppress
import re
import os
from dol.misc import get_obj
from dol import Files

ignore_if_module_not_found = suppress(ModuleNotFoundError, ImportError)
protocol_sep_p = re.compile('(\w+)://(.+)')

protocols = dict()


def get_local_file_bytes_or_folder_mapping(key):
    """Get byte contents given a filepath"""
    if key.startswith('file://'):
        key = key[len('file://') :]
    if os.path.isdir(key):
        return Files(key)
    else:
        with open(key, 'rb') as fp:
            # In case you're wondering if this closes fp:
            # https://stackoverflow.com/a/9885287/5758423
            return fp.read()


protocols['file'] = get_local_file_bytes_or_folder_mapping

with ignore_if_module_not_found:
    from haggle import KaggleDatasets

    kaggle_data = KaggleDatasets()

    def get_kaggle_data(key):
        """Get the zip object of a kaggle dataset (downloaded if not cached locally)"""
        if key.startswith('kaggle://'):
            key = key[len('kaggle://') :]
        return kaggle_data[key]

    protocols['kaggle'] = get_kaggle_data

with ignore_if_module_not_found:
    from graze import Graze

    graze = Graze().__getitem__
    protocols['http'] = graze
    protocols['https'] = graze


def grab(key):
    """Grab data from various protocols.

    >>> grab.prototols # doctest: +SKIP
    ['file', 'kaggle', 'http', 'https']
    >>> b = grab('https://raw.githubusercontent.com/i2mint/pyckup/master/LICENSE')
    >>> assert type(b) == bytes

    """
    if key.startswith('/') or key.startswith('\\'):
        key = 'file://' + key
    if '://' in key:
        m = protocol_sep_p.match(key)
        if m:
            protocol, ref = m.groups()
            protocol_func = protocols.get(protocol, None)
            if protocol_func is None:
                raise KeyError(f'Unrecognized protocol: {protocol}')
            else:
                return protocol_func(key)

    return get_obj(key)


grab.prototols = list(protocols)

import urllib

DFLT_USER_AGENT = 'Wget/1.16 (linux-gnu)'


def url_2_bytes(url, chk_size=1024, user_agent=DFLT_USER_AGENT):
    """get url content bytes"""

    def content_gen():
        req = urllib.request.Request(url)
        req.add_header('user-agent', user_agent)
        with urllib.request.urlopen(req) as response:
            while True:
                chk = response.read(chk_size)
                if len(chk) > 0:
                    yield chk
                else:
                    break

    return b''.join(content_gen())
