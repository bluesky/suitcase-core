import uuid
import pytest
from metadatastore.mds import MDS
import os

AUTH = os.environ.get('MDSTESTWITHAUTH', False)


@pytest.fixture(params=[1], scope='function')
def mds_all(request):
    '''Provide a function level scoped metadatastore instance talking to
    temporary database on localhost:27017 with focus on v1.
    '''
    db_name = "mds_testing_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017, timezone='US/Eastern',
                     mongo_user='tom',
                     mongo_pwd='jerry')
    version_v = request.param
    mds = MDS(test_conf, version_v, auth=AUTH)

    def delete_dm():
        print("DROPPING DB")
        mds._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return mds
