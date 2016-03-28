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
