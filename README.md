# chess

Mining chess data using PySpark

## Getting started

To save extracted data of openings
```console
export PYTHONPATH=.
spark-submit chess.py openings --save
```

## Datasets

Chess datasets can be found here:
- [Lichess](https://database.lichess.org/)

## Setup

- [Install Apache Spark on MacOS](https://notadatascientist.com/install-spark-on-macos/)
- [Update lo4j.properties](https://oleweidner.com/blog/2015/getting-started-with-spark-on-osx/)

For Mac, modify `.bashrc` or `.zshrc` files
```ini
PYSPARK_PYTHON=/usr/bin/python3
```