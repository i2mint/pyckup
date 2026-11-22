# pyckup

Grab data from various protocols.

A protocol is what’s to the left of `://` in a url.
You’ve seen’em. For example, `http` and `https`.
Well, if you try grabbing a valid url, you’ll get `bytes` out of it,
without having to make an actual http request yourself.

> ```pycon
> >>> b = grab('https://raw.githubusercontent.com/i2mint/pyckup/master/LICENSE')
> >>> isinstance(b, bytes)
> True
> ```

> Now let’s put those bytes in a file:

> ```pycon
> >>> from tempfile import mktemp
> >>> filepath = mktemp()
> >>> with open(filepath, 'wb') as fp:
> ...     n_bytes_written = fp.write(b);
> >>> assert n_bytes_written == len(b)
> ```

A filepath is a url. So you should be able to grab it’s contents too.

```pycon
>>> file_bytes = grab(filepath)
>>> assert file_bytes == b
```

There wasn’t really a protocol there, but when `grab` sees that the url
you’re passing it starts with a slash, it automatically prepends a `file://`
to it, like most browswers do.
You can also specify the `file://` protocol explicitly (again, like in
most browsers):

```pycon
>>> file_bytes_2 = grab('file://' + filepath)
>>> assert file_bytes_2 == file_bytes == b
```

If your filepath points to a folder, you’ll get back a `dol.Files` object,
which is a `Mapping` whose keys are file paths (relative to the folder) and
the values are the bytes (acquired lazily) of the corresponding file’s contents.

```pycon
>>> import os, typing
>>> folder, filename = os.path.dirname(filepath), os.path.basename(filepath)
>>> f = grab(folder)
>>> isinstance(f, typing.Mapping)
True
>>> filename in f
True
>>> file_bytes_3 = f[filename]
>>> assert file_bytes_3 == file_bytes_2 == file_bytes == b
```

`grab` can handle various protocols, according to what packages it finds
on your system. For example, if you have `haggle`
([https://pypi.org/project/haggle/](https://pypi.org/project/haggle/)) installed, you’ll find that `kaggle`
is also a valid protocol.

```pycon
>>> sorted(grab.dflt_protocols)
['file', 'http', 'https', 'kaggle']
```

Notice that `grab` has an argument called `protocols`.
Yes, this means you have control.
You just need to specify a mapping between protocol strings and the “url_to_content”
function that should be used to get the content from that url.

In the following we’ll add a fake `foo` protocol that doesn’t really fetch any
data, but applies `str.upper` to the url, but you get the point.

```pycon
>>> from functools import partial
>>> from pyckup.base import dflt_protocols
>>> mygrab = partial(grab, protocols=dict(dflt_protocols, foo=str.upper))
>>> mygrab('foo://a_fake_url')
'FOO://A_FAKE_URL'
```

### Modules

| [`base`](pyckup.base.html.md#module-pyckup.base)       | Base             |
|--------------------------------------------------------------------------------|------------------|
| [`sources`](pyckup.sources.html.md#module-pyckup.sources) | Data sources     |
| [`util`](pyckup.util.html.md#module-pyckup.util)       | Utils for pyckyp |
