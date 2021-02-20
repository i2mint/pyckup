from contextlib import suppress
import re
from py2store.misc import get_obj

protocol_sep_p = re.compile("(\w+)://(.+)")

protocols = dict()

with suppress(ModuleNotFoundError):
    def get_file_bytes(key):
        """Get byte contents given a filepath"""
        if key.startswith('file://'):
            key = key[len('file://'):]
        with open(key, 'rb') as fp:
            contents = fp.read()
        return contents


    protocols['file'] = get_file_bytes

with suppress(ModuleNotFoundError):
    from haggle import KaggleDatasets

    kaggle_data = KaggleDatasets()


    def get_kaggle_data(key):
        """Get the zip object of a kaggle dataset (downloaded if not cached locally)"""
        if key.startswith('kaggle://'):
            key = key[len('kaggle://'):]
        return kaggle_data[key]


    protocols['kaggle'] = get_kaggle_data

with suppress(ModuleNotFoundError):
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
    if '://' in key:
        m = protocol_sep_p.match(key)
        if m:
            protocol, ref = m.groups()
            protocol_func = protocols.get(protocol, None)
            if protocol_func is None:
                raise KeyError(f"Unrecognized protocol: {protocol}")
            else:
                return protocol_func(key)
    return get_obj(key)


grab.prototols = list(protocols)

import urllib

DFLT_USER_AGENT = "Wget/1.16 (linux-gnu)"


def url_2_bytes(
        url, chk_size=1024, user_agent=DFLT_USER_AGENT
):
    """get url content bytes"""
    def content_gen():
        req = urllib.request.Request(url)
        req.add_header("user-agent", user_agent)
        with urllib.request.urlopen(req) as response:
            while True:
                chk = response.read(chk_size)
                if len(chk) > 0:
                    yield chk
                else:
                    break

    return b"".join(content_gen())
