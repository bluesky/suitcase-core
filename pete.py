'''
Write a NeXus HDF5 data file from the scan data
'''

# set up the data broker (db)

import os

def setup_databroker():
    # this *should* come from ~/.config/filestore and ~/.config/metadatastore
    os.environ['MDS_HOST'] = 'localhost'
    os.environ['MDS_PORT'] = '27017'
    os.environ['MDS_DATABASE'] = 'metadatastore-production-v1'
    os.environ['MDS_TIMEZONE'] = 'US/Central'
    os.environ['FS_HOST'] = os.environ['MDS_HOST']
    os.environ['FS_PORT'] = os.environ['MDS_PORT']
    os.environ['FS_DATABASE'] = 'filestore-production-v1'
    
    # Connect to metadatastore and filestore.
    from metadatastore.mds import MDS, MDSRO
    from filestore.fs import FileStore, FileStoreRO
    from databroker import Broker
    mds_config = {'host': os.environ['MDS_HOST'],
                  'port': int(os.environ['MDS_PORT']),
                  'database': os.environ['MDS_DATABASE'],
                  'timezone': os.environ['MDS_TIMEZONE']}
    fs_config = {'host': os.environ['FS_HOST'],
                 'port': int(os.environ['FS_PORT']),
                 'database': os.environ['FS_DATABASE']}
    mds = MDSRO(mds_config)
    # For code that only reads the databases, use the readonly version
    #mds_readonly = MDSRO(mds_config)
    #fs_readonly = FileStoreRO(fs_config)
    fs = FileStoreRO(fs_config)
    db = Broker(mds, fs)
    
    return db, mds


def main():
    from suitcase.nexus import export
    db, mds = setup_databroker()
    
    header = db[-1]
    # print(header)
    # export(db[-1], 'meshscan4.h5', mds, use_uid=False)
    doc = header.start
    filename = '{}_{}.h5'.format(doc.beamline_id, doc.scan_id)
    if os.path.exists(filename):
        os.remove(filename)

    export(header, filename, mds, use_uid=False)


if __name__ == '__main__':
    main()
