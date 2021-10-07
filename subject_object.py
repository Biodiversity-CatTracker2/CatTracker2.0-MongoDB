import re
import uuid

import dateparser

from MongoDB_connect import mongodb_connect


def mkid(dictionary):
    id_ = uuid.uuid5(
        uuid.NAMESPACE_DNS, dictionary['subject_info']['cat_name'].lower())
    id_ = str(id_).split("-")[0]
    return id_


def make_subject_dict(cat_name):
    info = db['signup_form'].find_one({'cat_name': re.compile(cat_name, re.IGNORECASE)})
    del info['_id']
    del info['index']
    info['signup_timestamp'] = info['timestamp']
    del info['timestamp']
    owner_keys = ['known_cat_for', 'primary_caregiver', 'signup_timestamp'] +     [x for x in list(info.keys()) if x.startswith('owner')]
    subject = {
        'subject_info': {k: v for k, v in info.items() if k not in owner_keys},
        'owner_info': {k: v for k, v in info.items() if k in owner_keys},
        'tracking_info': {}
    }
    for k in [f'day_0{x}' for x in range(1, 8)]:
        subject['tracking_info'][k] = {
        'date': None,
        'complete': False,
        'no_of_data_points': None,
        'video_recorded_time': None,
        'data_is_exported': None
    }
    return subject

def update_existing(
    cat_name,
    day_num,
    date=None,
    complete=None,
    no_of_data_points=None,
    video_recorded_time=None,
    data_is_exported=None
):
    pre = 'info.tracking_info.day_0'
    updates_ = {
        f'{pre}{day_num}.date': dateparser.parse(date),
        f'{pre}{day_num}.complete': complete,
        f'{pre}{day_num}.no_of_data_points': no_of_data_points,
        f'{pre}{day_num}.video_recorded_time': video_recorded_time,
        f'{pre}{day_num}.data_is_exported': data_is_exported
    }
    updates = {k: v for k, v in updates_.items() if v is not None}
    db['subjects'].update_one(
        {
            'cat_name': cat_name
        },
        {
            '$set': updates
        }
    )
