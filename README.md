# chess

Mining chess data using PySpark

## Setup

[Install Apache Spark on MacOS](https://notadatascientist.com/install-spark-on-macos/)
[Update lo4j.properties](https://oleweidner.com/blog/2015/getting-started-with-spark-on-osx/)

For Mac, modify `.bashrc` or `.zshrc` files
```ini
PYSPARK_PYTHON=/usr/bin/python3
```

## Getting started

To run a script
```console
spark-submit top_100_openings.py
```

## Files

Avoid committing large files.

Chess datasets can be found here:
- [Lichess](https://database.lichess.org/)