import json
from pathlib import Path

import keyring
import psutil
from ssh_pymongo import MongoSession


def check_openconnect():
    for p in psutil.process_iter():
        try:
            if p.name() == 'openconnect':
                return p
        except psutil.ZombieProcess:
            continue


def mongodb_connect():
    if not check_openconnect():
        raise ProcessLookupError('Not connected to Anvil VPN!')
    host = keyring.get_password('mongodb', 'host')
    username = keyring.get_password('mongodb', 'username')
    passwd = keyring.get_password('mongodb', username)
    key = f'{Path.home()}/.ssh/anvil_key'
    session = MongoSession(
        host,
        port=22,
        user='ubuntu',
        key=key,
        uri=f'mongodb://{username}:{passwd}@{host}:27017/?authSource=admin')
    return session
