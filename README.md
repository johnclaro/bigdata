# bigdata

Explore datasets using PySpark

## Setup

[Install Apache Spark on MacOS](https://notadatascientist.com/install-spark-on-macos/)
[Update lo4j.properties](https://oleweidner.com/blog/2015/getting-started-with-spark-on-osx/)

For Mac, modify `.bashrc` or `.zshrc` files
```ini
PYSPARK_PYTHON=/usr/bin/python3
```

Each project has a `files` folder inside them, e.g. `fk/files`

## Getting started

To run a script
```console
spark-submit fk/ratings.py
```

## Files

Avoid committing files that are over 500KB.

Download these large files:
- [fk/files/ratings](https://files.grouplens.org/datasets/movielens/ml-100k.zip)