# bigdata

Explore datasets using PySpark

## Setup

[Install Apache Spark on MacOS](https://notadatascientist.com/install-spark-on-macos/)

For Mac, modify `.bashrc` or `.zshrc` files
```ini
PYSPARK_PYTHON=/usr/bin/python3
```

Each project has a datasets folder inside them, e.g. `fk/datasets`

## Getting started

To run a script
```console
spark-submit fk/ratings.py
```

## Datasets

Avoid committing files that are over 500KB.

Download these large datasets:
- [fk/datasets/ratings](https://files.grouplens.org/datasets/movielens/ml-100k.zip)