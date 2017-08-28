import uuid
import pytest
from databroker import Broker
import os

AUTH = os.environ.get('MDSTESTWITHAUTH', False)


test_config = {
    'metadatastore': {
        'module': 'databroker.headersource.mongo',
        'class': 'MDS',
        'config': {
            'host': 'localhost',
            'port': 27017,
            'database': 'test1',
            'timezone': 'US/Eastern'}
    },
    'assets': {
        'module': 'databroker.assets.mongo',
        'class': 'Registry',
        'config': {
            'host': 'localhost',
            'port': 27017,
            'database': 'test2'}
    }
}


@pytest.fixture(params=[1], scope='function')
def db_all(request):
    '''Provide a function level scoped metadatastore instance talking to
    temporary database on localhost:27017 with focus on v1.
    '''
    db = Broker.from_config(test_config)

    def delete_dm():
        print("DROPPING DB")
        db.mds._connection.drop_database('test1')
        db.mds._connection.drop_database('test2')

    request.addfinalizer(delete_dm)

    return db
