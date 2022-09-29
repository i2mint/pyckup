"""
Base
"""
from contextlib import suppress
import re
import os
from dol.misc import get_obj
from dol import Files

ignore_if_module_not_found = suppress(ModuleNotFoundError, ImportError)
protocol_sep_p = re.compile(r'(\w+)://(.+)')

dflt_protocols = dict()


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


dflt_protocols['file'] = get_local_file_bytes_or_folder_mapping

with ignore_if_module_not_found:
    from haggle import KaggleDatasets

    kaggle_data = KaggleDatasets()

    def get_kaggle_data(key):
        """Get the zip object of a kaggle dataset (downloaded if not cached locally)"""
        if key.startswith('kaggle://'):
            key = key[len('kaggle://') :]
        return kaggle_data[key]

    dflt_protocols['kaggle'] = get_kaggle_data

with ignore_if_module_not_found:
    from graze import Graze

    graze = Graze().__getitem__
    dflt_protocols['http'] = graze
    dflt_protocols['https'] = graze


def grab(key, protocols=None):
    """
    Grab data from various protocols.

    A protocol is what's to the left of ``://`` in a url.
    You've seen'em. For example, ``http`` and ``https``.
    Well, if you try grabbing a valid url, you'll get ``bytes`` out of it,
    without having to make an actual http request yourself.

        >>> b = grab('https://raw.githubusercontent.com/i2mint/pyckup/master/LICENSE')
        >>> isinstance(b, bytes)
        True

        Now let's put those bytes in a file:


        >>> from tempfile import mktemp
        >>> filepath = mktemp()
        >>> with open(filepath, 'wb') as fp:
        ...     n_bytes_written = fp.write(b);
        >>> assert n_bytes_written == len(b)

    A filepath is a url. So you should be able to grab it's contents too.

        >>> file_bytes = grab(filepath)
        >>> assert file_bytes == b

    There wasn't really a protocol there, but when ``grab`` sees that the url
    you're passing it starts with a slash, it automatically prepends a ``file://``
    to it, like most browswers do.
    You can also specify the ``file://`` protocol explicitly (again, like in
    most browsers):

        >>> file_bytes_2 = grab('file://' + filepath)
        >>> assert file_bytes_2 == file_bytes == b

    If your filepath points to a folder, you'll get back a ``dol.Files`` object,
    which is a ``Mapping`` whose keys are file paths (relative to the folder) and
    the values are the bytes (acquired lazily) of the corresponding file's contents.

        >>> import os, typing
        >>> folder, filename = os.path.dirname(filepath), os.path.basename(filepath)
        >>> f = grab(folder)
        >>> isinstance(f, typing.Mapping)
        True
        >>> filename in f
        True
        >>> file_bytes_3 = f[filename]
        >>> assert file_bytes_3 == file_bytes_2 == file_bytes == b

    ``grab`` can handle various protocols, according to what packages it finds
    on your system. For example, if you have ``haggle``
    (https://pypi.org/project/haggle/) installed, you'll find that ``kaggle``
    is also a valid protocol.

        >>> sorted(grab.dflt_protocols) # doctest: +SKIP
        ['file', 'http', 'https', 'kaggle']

    Notice that ``grab`` has an argument called ``protocols``.
    Yes, this means you have control.
    You just need to specify a mapping between protocol strings and the "url_to_content"
    function that should be used to get the content from that url.

    In the following we'll add a fake ``foo`` protocol that doesn't really fetch any
    data, but applies ``str.upper`` to the url, but you get the point.

        >>> from functools import partial
        >>> from pyckup.base import dflt_protocols
        >>> mygrab = partial(grab, protocols=dict(dflt_protocols, foo=str.upper))
        >>> mygrab('foo://a_fake_url')
        'FOO://A_FAKE_URL'

    """
    if callable(protocols):
        protocols = protocols()
    protocols = protocols or dflt_protocols

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


grab.dflt_protocols = list(dflt_protocols)

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
