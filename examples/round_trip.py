import logging
import os

from suitcase.spec import Specfile
from suitcase.tests.test_spec import _round_trip, stream_handler

# turn down the logging level
stream_handler.setLevel(logging.INFO)
suitcase_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data = os.path.join(suitcase_dir, 'suitcase', 'tests', 'data', '20160219.spec')

sf = Specfile(data)

round_tripped = _round_trip(sf)
print('Round tripped specfile saved to: {}'.format(round_tripped.filename))
