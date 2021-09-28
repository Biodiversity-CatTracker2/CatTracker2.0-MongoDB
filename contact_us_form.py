import json
from pathlib import Path

import imap_tools
import keyring
from ssh_pymongo import MongoSession


def mongodb_connect():
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


def main():
    addr = 'malyeta@ncsu.edu'
    mail_p = keyring.get_password('ncsu.gmail', addr)
    session = mongodb_connect()
    db = session.connection['cattracker2']
    db['contact_us'].drop()

    with imap_tools.MailBox('imap.gmail.com').login(addr, mail_p) as mailbox:
        for msg in mailbox.fetch(imap_tools.A(from_='wordpress@cattracker.org')):
            include = [
                'date_str', 'flags', 'from_', 'reply_to', 'size', 'subject',
                'text', 'to', 'uid'
            ]
            msg_ = [(x, getattr(msg, x)) for x in dir(msg) if x in include]
            data = {k: v for k, v in msg_}
            for k, v in msg_:
                try:
                    if len(v) == 1:
                        data.update({k: v[0]})
                except TypeError:
                    pass
            db['contact_us'].insert(data)
    session.stop()


if __name__ == '__main__':
    main()
