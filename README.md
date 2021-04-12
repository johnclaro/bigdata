# bigdata

Explore datasets using PySpark

## Setup

[Install Apache Spark on MacOS](https://notadatascientist.com/install-spark-on-macos/)

For Mac, modify `.bashrc` or `.zshrc` files
```ini
PYSPARK_PYTHON=/usr/bin/python3
```

Each project has a datasets folder inside them, e.g. `movielens/datasets`

## Getting started

To run a script
```console
spark-submit movielens/ratings.py
```

## Datasets

Avoid committing files that are over 500KB.

Download these large datasets:
- [movielens/datasets/ratings](https://files.grouplens.org/datasets/movielens/ml-100k.zip)