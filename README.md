# pyckup

Tools to provide easy access to prepared data to data scientists that can't 
be asked.

They just want to get on with the fun -- **not** get stuck in data access and 
data preparation concerns. And they should want that!

Of course, someone needed to do the work of getting the data from where and how it is, 
to where and how it needs to be (for a particular problem and context). 

What we believe is that this work should not only be less tedious and less 
time-consuming (see py2store and related for that!), but also, once it's done, 
it shouldn't have to be re-done every time someone wants to kick the data 
around. 

So we made ``pyckup``. 

We hope it helps.

# install

```
pip install pyckup
```

# Usage

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


# Examples

```python
from pyckup import grab
```

See what (default) protocols you have access to.

```python
from pyckup import grab

grab.procotols
# ['file', 'kaggle', 'http', 'https']
```

## Grab file contents

Specifying a "file" protocol (i.e. prefixing your string with 
"file://" -- followed by a full path) will give you the contents of the file 
in bytes.

```python
from pyckup import grab

b = grab('file:///Users/Thor.Whalen/Dropbox/dev/p3/proj/i/pyckup/pyckup/__init__.py')
assert isinstance(b, bytes)
print(b.decode())
# from pyckup.base import grab, protocols
```

But you can also use a full path, or other natural means of specifying files.
In that case though, `grab` will try to give you the contents in a convenient type 
(e.g. a `dict` for `.json`, a python object of `.pickle`, string for `.txt`...).
This is convenient, but don't depend on the type to strongly
 since it depends on what `py2store.misc` sets it to be. 

```python
from pyckup import grab

grab('/Users/Thor.Whalen/Dropbox/dev/p3/proj/i/pyckup/pyckup/__init__.py')
# b'from pyckup.base import grab, protocols\n\n\n'
grab('~/Dropbox/dev/p3/proj/i/pyckup/data/example.json')
# {'hello': 'world', 'abc': [1, 2, 3]}
grab('~/Dropbox/dev/p3/proj/i/pyckup/data/example.pickle')
# [1, 2, 3]
print(grab('~/Dropbox/dev/p3/proj/i/pyckup/data/example.txt'))
# This
# is
# text
```

## Grab the contents of a url

```python
from pyckup import grab

b = grab('https://raw.githubusercontent.com/i2mint/pyckup/master/LICENSE')
type(b), len(b)
# (bytes, 11357)
print(b[:100].decode())
#                                  Apache License
#                            Version 2.0, January 2004
```

## Grab stuff from kaggle 

Note: If you want to access kaggle datasets with ``pyckup``, 
you'll need to get an account. 
See [haggle](https://github.com/otosense/haggle#api-credentials) 
for more information.


```python
from pyckup import grab

z = grab('kaggle://drgilermo/face-images-with-marked-landmark-points')
list(z)
# ['face_images.npz', 'facial_keypoints.csv']
print(z['facial_keypoints.csv'][:100].decode())
# left_eye_center_x,left_eye_center_y,right_eye_center_x,right_eye_center_y,left_eye_inner_corner_x,le
```


