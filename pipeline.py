from Contact_Us_Form import contact_us_form
from Google_Forms import google_forms
from Parse_Emails import parse_emails
from MongoDB_connect import mongodb_connect


def main(keep_session=False):
    session = mongodb_connect()
    print('Connected to database...')
    contact_us_form(session)
    print('Finished running `contact_us_form()`...')
    google_forms(session)
    print('Finished running `google_forms()`...')
    parse_emails(session)
    print('Finished running `parse_emails()`...')
    print('Done!')
    if not keep_session:
        session.close()
    else:
        return session


if __name__ == '__main__':
    main()
