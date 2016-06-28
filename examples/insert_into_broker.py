from suitcase.spec import Specfile, insert_specfile_into_broker
from suitcase.tests.test_spec import spec_filename, stream_handler
import logging

# turn down the logging level
stream_handler.setLevel(logging.INFO)

sf = Specfile(spec_filename())

insert_specfile_into_broker(sf)
insert_specfile_into_broker(sf)

