from __future__ import division
import uuid

import numpy as np
from functools import wraps
import time as ttime
# "Magic numbers" for this simulation
start, stop, step, points_per_step = 0, 6, 1, 7
deadband_size = 0.9
num_exposures = 17

def stepped_ramp(start, stop, step, points_per_step, noise_level=0.1):
    """
    Simulate a stepped ramp.
    """
    rs = np.random.RandomState(0)
    data = np.repeat(np.arange(start, stop, step), points_per_step)
    noise = step * noise_level * rs.randn(len(data))
    noisy_data = data + noise
    return noisy_data


def apply_deadband(data, band):
    """
    Turn a stream of regularly spaced data into an intermittent stream.

    This simulates a deadband, where each data point is only included if
    it is significantly different from the previously included data point.

    Parameters
    ----------
    data : ndarray
    band : float
        tolerance, the width of the deadband, must be greater than 0. Raises a
        ValueError if band is less than 0

    Returns
    -------
    result : tuple
        indicies, data
    """
    if band < 0:
        raise ValueError("The width of the band must be nonnegative.")
    # Eric and Dan can't think of a way to vectorize this.
    set_point = data[0]
    # Always include the first point.
    indicies = [0]
    significant_data = [data[0]]
    for i, point in enumerate(data[1:]):
        if abs(point - set_point) > band:
            indicies.append(1 + i)
            significant_data.append(point)
            set_point = point
    return indicies, significant_data


def example(func):
    @wraps(func)
    def mock_run_start(mds, run_start_uid=None, sleep=0, make_run_stop=True):
        if run_start_uid is None:
            run_start_uid = mds.insert_run_start(time=ttime.time(), scan_id=1,
                                                 beamline_id='example',
                                                 uid=str(uuid.uuid4()))

        # these events are already the sanitized version, not raw mongo objects
        events = func(mds, run_start_uid, sleep)
        # Infer the end run time from events, since all the times are
        # simulated and not necessarily based on the current time.
        time = max([event['time'] for event in events])
        if make_run_stop:
            run_stop_uid = mds.insert_run_stop(run_start_uid, time=time,
                                               exit_status='success',
                                               uid=str(uuid.uuid4()))
            run_stop, = mds.find_run_stops(uid=run_stop_uid)
        return events
    return mock_run_start


@example
def run(mds, run_start_uid=None, sleep=0):
    if sleep != 0:
        raise NotImplementedError("A sleep time is not implemented for this "
                                  "example.")
    # Make the data
    ramp = stepped_ramp(start, stop, step, points_per_step)
    deadbanded_ramp = apply_deadband(ramp, deadband_size)
    rs = np.random.RandomState(5)
    point_det_data = rs.randn(num_exposures) + np.arange(num_exposures)

    # Create Event Descriptors
    data_keys1 = {'point_det': dict(source='PV:ES:PointDet', dtype='number'),
                  'boolean_det': dict(source='PV:ES:IntensityDet', dtype='string'),
                  'ccd_det_info': dict(source='PV:ES:CCDDet', dtype='list')}
    data_keys2 = {'Tsam': dict(source='PV:ES:Tsam', dtype='number')}
    ev_desc1_uid = mds.insert_descriptor(run_start=run_start_uid,
                                         data_keys=data_keys1,
                                         time=ttime.time(),
                                         uid=str(uuid.uuid4()),
                                         name='primary')
    ev_desc2_uid = mds.insert_descriptor(run_start=run_start_uid,
                                         data_keys=data_keys2,
                                         time=ttime.time(),
                                         uid=str(uuid.uuid4()),
                                         name='baseline')

    # Create Events.
    events = []

    # Point Detector Events
    base_time = ttime.time()
    for i in range(num_exposures):
        time = float(2 * i + 0.5 * rs.randn()) + base_time
        data = {'point_det': point_det_data[i],
                'boolean_det': 'Yes',
                'ccd_det_info': ['on', 'off']}
        timestamps = {'point_det': time,
                      'boolean_det': time,
                      'ccd_det_info': time}
        event_dict = dict(descriptor=ev_desc1_uid, seq_num=i,
                          time=time, data=data, timestamps=timestamps,
                          uid=str(uuid.uuid4()))
        event_uid = mds.insert_event(**event_dict)
        # grab the actual event from metadatastore
        event, = mds.find_events(uid=event_uid)
        events.append(event)
        assert event['data'] == event_dict['data']

    # Temperature Events
    for i, (time, temp) in enumerate(zip(*deadbanded_ramp)):
        time = float(time) + base_time
        data = {'Tsam': temp}
        timestamps = {'Tsam': time}
        event_dict = dict(descriptor=ev_desc2_uid, time=time,
                          data=data, timestamps=timestamps, seq_num=i,
                          uid=str(uuid.uuid4()))
        event_uid = mds.insert_event(**event_dict)
        event, = mds.find_events(uid=event_uid)
        events.append(event)
        assert event['data'] == event_dict['data']

    return events
