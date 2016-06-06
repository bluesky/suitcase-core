[![Build Status](https://travis-ci.org/NSLS-II/suitcase.svg?branch=master)](https://travis-ci.org/NSLS-II/suitcase)
[![codecov.io](https://codecov.io/github/NSLS-II/suitcase/coverage.svg?branch=master)](https://codecov.io/github/NSLS-II/suitcase?branch=master)
[![Anaconda-Server Badge](https://anaconda.org/lightsource2/suitcase/badges/version.svg)](https://anaconda.org/lightsource2/suitcase)
[![Anaconda-Server Badge](https://anaconda.org/lightsource2/suitcase/badges/installer/conda.svg)](https://conda.anaconda.org/lightsource2)

# Suitcase

Suitcase contains tools for exporting data from NSLS-II. It aims to support
two important use cases:

1. Export all data and metadata to an HDF5 file. In principle this file can be
   organized any way the user desires. For now, the file reflects the NSLS-II
   Document specification, but we are *not* promoting this as a standard
   exchange format. There are plans to support (possibly lossy) conversion to
   Data Exchange and Nexus formats.
2. Export all data and metadata in a portable "Data Broker" that uses can run
   on their own computers with minimal dependencies. (Specifically, the full-
   fledged Data Broker runs a mongo database. The portable broker runs on
   sqlite, which is built in to Python and thus requires much less setup.)

Number 2 is planned but not yet implemented.

## Export headers and data into a hdf file

```python
from databroker import db
from suitcase import hdf

# find the header(s) that you want to export
hdrs = db(start_time='2016-03-03', stop_time='2016-03-05')
fname = '/path/to/output/data'
hdf.export(hdrs, fname)
```

## Inserting data in the spec format into the databroker

This functionality is provided so that data that has been collected with spec
can be inserted into the databroker stack which allows the analysis tools that
are being written at NSLS-II to be used with this sort of "legacy" data. This
also has the advantage that "legacy" data can be analyzed along side data that
was collected with bluesky. Note that there are a number of checks to make sure
that you do not add data more than one time.


```python
from suitcase import spec
specfile = spec.Specfile('/path/to/specfile')

# Insert the whole specfile into the databroker
spec.insert_into_broker(specfile)

# Insert a single scan into the databroker
scan_id = 1
specscan = specfile[scan_id]
spec.insert_into_broker(specscan)
```
