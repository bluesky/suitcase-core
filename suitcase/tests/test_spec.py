from __future__ import absolute_import, print_function, division
from suitcase.spec import Specfile
import pytest
import os


@pytest.fixture(scope='module')
def data():
    path = os.path.join(os.path.dirname(__file__), 'data', '20160219.spec')
    return Specfile(path)


def test_spec_parsing(data):
    assert len(data) == 34
