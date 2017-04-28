.. _api_changes:

=============
 API changes
=============

Non-backward compatible API changes


v0.5.2
======


``suitcase.hdf5.export``
------------------------

Replaced input parameter mds with db in ``export`` function. db=None is set as default and put at
the end of the input argument list. This change will facilitate the usage of
other functions of databroker, intead of only those from metadatastore.

The future version of header file will include databroker, like hdr.db. This
update will be taken care of later.
