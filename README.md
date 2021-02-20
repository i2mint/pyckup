# pyckup

Tools to provide easy access to prepared data to data scientists that can't 
be asked.

They just want to get on with the fun -- **not** get stuck in data access and 
data preparation concerns. And they should want that!

Of course, someone needed to do the work of getting the data from where and how it is, 
to where and how it needs to be (for a particular problem and context). 

What we believe is that this work should not only be less tedious and less time 
consuming (see py2store and related for that!), but also, once it's done, 
it shouldn't have to be re-done every time some one wants to kick the data 
around. 

So we made pyckup. 

We hope it helps.

# install

```
pip install pyckup
```

Note: If you want to access kaggle datasets with pyckup, you'll need to get an account. 
See [haggle](https://github.com/otosense/haggle#api-credentials) for more information.

# Examples

```python
from pyckup import grab
```

See what protocols you have access to.

```python
from pyckup import grab

grab.prototols
# ['file', 'kaggle', 'http', 'https']
```

## Grab file contents

Specifying a "file" protocol (i.e. prefexing your string with "file://" -- followed by a full path) 
will give you the contents of the file in bytes.

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

Note: You need to have an account -- see pypi haggle for more details.

```python
from pyckup import grab

z = grab('kaggle://drgilermo/face-images-with-marked-landmark-points')
list(z)
# ['face_images.npz', 'facial_keypoints.csv']
print(z['facial_keypoints.csv'][:100].decode())
# left_eye_center_x,left_eye_center_y,right_eye_center_x,right_eye_center_y,left_eye_inner_corner_x,le
```


