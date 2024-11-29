#!/bin/bash
set -e

# Import the data dump
psql -v --username postgres --dbname postgres < /data.dump
