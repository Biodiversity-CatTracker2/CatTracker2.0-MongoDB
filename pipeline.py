from Contact_Us_Form import contact_us_form
from Google_Forms import google_forms
from Parse_Emails import parse_emails
from MongoDB_connect import mongodb_connect


def main():
    session = mongodb_connect()
    contact_us_form(session)
    google_forms(session)
    parse_emails(session)


if __name__ == '__main__':
    main()
