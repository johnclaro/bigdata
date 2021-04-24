# chess

Mining chess data using PySpark

## Getting started

To extract `openings` data for dataset `lichess_db_standard_rated_2013-01.pgn` 
and save as a file
```console
export PYTHONPATH=.
spark-submit chess.py openings -f lichess_db_standard_rated_2013-01.pgn -s
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