# Infrastructure Track Week 5 Data Quality Repository

This week, we'll be covering week-over-week row count checks and other data quality fundamentals.

We'll be using:

- `pytest`
- `pyspark[sql]`
- `chispa`

We're going to be covering week-over-week row count checks and other data quality fundamentals.

## Getting Started

Very first thing, if you haven't installed Java yet, please do so [here](https://www.java.com/en/download/help/download_options.html)

Then you'll want to create a virtual environment for Python 3.11

- `virtualenv --python=python3.11 .venv`
- `source .venv/bin/activate`

Then you'll want to `pip install -r requirements.txt` this will bring in `chispa`, `pytest` and `pyspark`

Then you'll want to run `pytest` 