#!/usr/bin/env python
# coding: utf-8

import bz2
import datetime
import io
import re
import sys
import tempfile
import traceback
import uuid
from glob import glob
from pathlib import Path

import bson
import dateparser
import gridfs

from MongoDB_connect import mongodb_connect


def mkid(dictionary):
    id_ = uuid.uuid5(uuid.NAMESPACE_DNS,
                     dictionary['subject_info']['cat_name'].lower())
    id_ = str(id_).split("-")[0]
    return id_


def make_subject_dict(cat_name):
    info = db['signup_form'].find_one(
        {'cat_name': re.compile(cat_name, re.IGNORECASE)})
    del info['_id']
    del info['index']
    info['signup_timestamp'] = info['timestamp']
    del info['timestamp']
    owner_keys = ['known_cat_for', 'primary_caregiver', 'signup_timestamp'
                  ] + [x for x in list(info.keys()) if x.startswith('owner')]
    subject = {
        'subject_info': {k: v
                         for k, v in info.items() if k not in owner_keys},
        'owner_info': {k: v
                       for k, v in info.items() if k in owner_keys},
        'tracking_info': {}
    }
    for k in [f'day_0{x}' for x in range(1, 8)]:
        subject['tracking_info'][k] = {'date': None, 'complete': False}
        if x > 2:
            subject['tracking_info'][k].update({
                'stopped_at': None,
                'recorded_video_time': None,
                'data_is_exported': None
            })
    return subject


def mk_binary(file_path):
    with open(file_path, 'rb') as f:
        file_object = bson.binary.Binary(f.read())
        return file_object


def add_subject(cat_name, simple_id, tag_is_with_owner=True):
    subject = make_subject_dict(cat_name)
    try:
        db['subjects'].insert({
            '_id':
            subject['subject_info']['cat_name'].lower() + '-' + mkid(subject),
            'simple_id':
            simple_id,
            'cat_name':
            subject['subject_info']['cat_name'].lower(),
            'tag_is_with_owner':
            tag_is_with_owner,
            'info':
            subject
        })
    except pymongo.errors.DuplicateKeyError as e:
        print(e)


def load_data_file(cat_name, day, data_file_type):
    query = db['subjects'].find_one({'cat_name': cat_name})
    query_res = query['info']['tracking_info'][day]['data']
    with io.BytesIO(query_res) as f:
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(f.read())
            with bz2.BZ2File(temp.name, 'rb') as bzf:
                df = pd.read_csv(bzf)
    return df


def update_existing(cat_name,
                    day_num,
                    date=None,
                    complete=None,
                    stopped_at=None,
                    recorded_video_time=None,
                    data_is_exported=None,
                    data=None,
                    dry_run=False):
    if not date:
        res_ = db['subjects'].find_one({'cat_name': 'kraken'})
        date = str(res_['info']['tracking_info'][f'day_0{day_num}']['date'])
    if stopped_at:
        stopped_at = dateparser.parse(stopped_at).replace(
            tzinfo=None).astimezone(tz=datetime.timezone.utc)
    pre = 'info.tracking_info.day_0'
    files = {}
    if data:
        for k, v in data.items():
            _id = f'{pre}{day_num}.{k}'
            try:
                file_id = fs.put(mk_binary(v[1]), _id=_id, file_name=k)
            except:
                db['fs.files'].delete_one({'_id': _id})
                db['fs.chunks'].delete_many({'files_id': _id})
                file_id = fs.put(mk_binary(v[1]), _id=_id, file_name=k)
            files.update({v[0]: file_id})

    updates_ = {
        f'{pre}{day_num}.date': dateparser.parse(date),
        f'{pre}{day_num}.complete': complete,
        f'{pre}{day_num}.stopped_at': stopped_at,
        f'{pre}{day_num}.recorded_video_time': recorded_video_time,
        f'{pre}{day_num}.data_is_exported': data_is_exported,
        f'{pre}{day_num}.data': files
    }

    updates = {k: v for k, v in updates_.items() if v is not None}

    if not dry_run:
        db['subjects'].update_one({'cat_name': cat_name}, {'$set': updates})
    return updates
