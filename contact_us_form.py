import json
from pathlib import Path

import imap_tools
import keyring
from tqdm import tqdm
from ssh_pymongo import MongoSession

from MongoDB_connect import mongodb_connect


def contact_us_form(session=None):
    addr = keyring.get_password('emails', 'ncsu')
    mail_p = keyring.get_password('ncsu.gmail', addr)
    spawned_ssession = False
    if not session:
        session = mongodb_connect()
        spawned_ssession = True
    db = session.connection['cattracker2']
    db['contact_us'].drop()

    with imap_tools.MailBox('imap.gmail.com').login(addr, mail_p) as mailbox:
        mails = mailbox.fetch(
            criteria=imap_tools.A(from_='wordpress@cattracker.org'), bulk=True)
        for msg in mails:
            include = [
                'date', 'flags', 'from_', 'reply_to', 'size', 'subject',
                'text', 'to', 'uid'
            ]
            msg_ = [(x, getattr(msg, x)) for x in dir(msg) if x in include]
            data = {k: v for k, v in msg_}
            data['uid'] = int(data['uid'])
            body = []
            for line in data['text'].split('\n'):
                if not any([
                        line.startswith(x)
                        for x in ['From:', 'Subject:', 'Message Body:']
                ]):
                    body.append(line)
            if body[0] == '\r':
                body = body[1:]
            data['text'] = ''.join(body)
            for k, v in msg_:
                try:
                    if len(v) == 1:
                        data.update({k: v[0]})
                except TypeError:
                    pass
            db['contact_us'].insert(data)
    if spawned_ssession:
        session.stop()


if __name__ == '__main__':
    contact_us_form()
