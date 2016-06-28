import logging

from suitcase.spec import Specfile
from suitcase.tests.test_spec import _round_trip, stream_handler, spec_filename

# turn down the logging level
stream_handler.setLevel(logging.INFO)

sf = Specfile(spec_filename())

round_tripped = _round_trip(sf)
print('Round tripped specfile saved to: {}'.format(round_tripped.filename))
