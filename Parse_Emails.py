import re

import keyring
from bs4 import BeautifulSoup
from imap_tools import A, MailBox, O

from MongoDB_connect import mongodb_connect


def parse_emails(session=None):
    spawned_ssession = False
    if not session:
        session = mongodb_connect()
        spawned_ssession = True
    db = session.connection['cattracker2']

    contacted_us = list(
        set(x['reply_to'].lower()
            for x in db.contact_us.find({'reply_to': {
                '$ne': None
            }})))

    addr = keyring.get_password('emails', 'ncsu')
    mail_p = keyring.get_password('ncsu.gmail', addr)
    emails_mapping = list(
        map(lambda x: A(from_=x), contacted_us + ['wordpress@cattracker.org']))

    with MailBox('imap.gmail.com').login(addr, mail_p) as mailbox:
        mails = mailbox.fetch(criteria=A(O(*emails_mapping)), bulk=True)
        db['emails'].drop()
        for msg in mails:
            include = [
                'date', 'from_', 'reply_to', 'to', 'subject', 'text', 'flags',
                'size', 'uid'
            ]
            msg_ = [(x, getattr(msg, x)) for x in dir(msg) if x in include]
            data = {k: v for k, v in msg_}
            for k, v in msg_:
                try:
                    if len(v) == 1:
                        data.update({k: v[0]})
                except TypeError:
                    pass

            mail_regex = r'On\s[a-zA-Z]+,\s[a-zA-Z]+\s+[0-9]+,\s[0-9]+'
            gmail_regex = r'-+\s[a-zA-Z]+\s+[a-zA-Z]+\s-+'
            content = re.split(mail_regex, data['text'])
            if len(content) == 1:
                content = re.split(gmail_regex, data['text'])

            body = []
            for line in content[0].split('\n'):
                if not any([
                        line.startswith(x)
                        for x in ['From:', 'Subject:', 'Message Body:']
                ]):
                    body.append(line)

            data['text'] = '\n'.join(body)
            data = {k: data[k] for k in include}
            data['_id'] = data['uid']
            db['inbox_emails'].update_one({'_id': data['uid']}, {'$set': data},
                                          upsert=True)

            mailbox.box.uid('STORE', data['uid'], 'X-GM-LABELS',
                            'cat-tracker-2.0')
    if spawned_ssession:
        session.stop()


if __name__ == '__main__':
    parse_emails()
