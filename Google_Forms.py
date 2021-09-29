import json
import re
import statistics
from pathlib import Path

import keyring
import pandas as pd
import pygsheets
from ssh_pymongo import MongoSession

from MongoDB_connect import mongodb_connect


def get_form_data(client_secret, form_name):
    gc = pygsheets.authorize(client_secret=client_secret)
    sh = gc.open(form_name)
    return sh


def clean_responses(df):
    sep_cols = df[df.columns[3]].str.split(',', n=2, expand=True)
    sep_cols.rename(columns={
        0: 'breed',
        1: 'coat_color',
        2: 'eye_color'
    },
        inplace=True)
    df = pd.concat([df.iloc[:, :3], sep_cols, df.iloc[:, 4:]], axis=1)

    new_names = [
        'timestamp', 'cat_name', 'cat_age', 'breed', 'coat_color', 'eye_color',
        'cat_weight', 'cat_sex', 'known_cat_for', 'primary_caregiver',
        'cat_brings_dead_prey', 'cat_has_free_access_to_outside',
        'frequency_of_cat_going_outside', 'owner_name', 'owner_year_of_birth',
        'owner_home_address', 'owner_email_address', 'owner_phone_number',
        'additional_info'
    ]
    df = df.rename(columns={k: v for k, v in zip(df.columns, new_names)})

    df.loc[df.primary_caregiver == 'Yes', 'primary_caregiver'] = True

    cat_age = [[int(x) for x in list(str(df.cat_age[n])) if x.isdigit()]
               for n in range(len(df.cat_age))]
    df.cat_age = [x[0] if len(x) == 1 else statistics.mean(x) for x in cat_age]

    answers_ = [re.split(r'\W', x.lower()) for x in df['cat_brings_dead_prey']]
    answers = []
    for x, y in zip(answers_, df['cat_brings_dead_prey']):
        if 'no' in x:
            answers.append(False)
        elif any(z in 'yes' for z in x):
            y = y.lower().replace(x[0], '').replace('.', '').replace(',', '')
            answers.append([True, y.rstrip().lstrip()])
        else:
            answers.append(y)
    df.cat_brings_dead_prey = answers

    df.owner_phone_number = [
        int(str(x).replace('-', '')) for x in df['owner_phone_number']
    ]

    free_access = []
    for x in df['cat_has_free_access_to_outside']:
        if 'partially restricted' in x:
            free_access.append(2)
        elif 'yes' in x.lower():
            free_access.append(1)
        elif 'no' in x.lower():
            free_access.append(0)
    df['cat_has_free_access_to_outside'] = free_access

    df.owner_name = [x.title() for x in df.owner_name]
    df.cat_name = df.cat_name.str.capitalize()

    return df


def google_forms(session=None):
    sh = get_form_data('client_secret.json',
                       'Cat Tracker 2.0 – Sign-up (Responses)')
    df = sh.sheet1.get_as_df()
    df = clean_responses(df)
    data = json.loads(df.to_json(orient='index'))
    spawned_ssession = False
    if not session:
        session = mongodb_connect()
        spawned_ssession = True
    db = session.connection['cattracker2']
    db['signup_form'].drop()
    for k, v in data.items():
        d = {'index': int(k)}
        d.update(v)
        db['signup_form'].insert(d)
    if spawned_ssession:
        session.stop()


if __name__ == '__main__':
    google_forms()
