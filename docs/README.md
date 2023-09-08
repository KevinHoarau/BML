# How to build the documentation

First, install BML in editable mode:
```
$ pip install --editable .
```

Then, move into the docs folder and install the dependencies:
```
$ cd docs
$ pip install -r requirements.txt
```

Build the documentation:
```
$ ./build_docs.sh
```

To update the documentation on github:
```
$ cd ..
$ ./update_gh-pages.sh
```