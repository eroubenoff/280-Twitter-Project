# Import smtplib for the actual sending function
import smtplib
from credentials import email_auth
from email.mime.text import MIMEText


def send_error_email(e):
    # Open a plain text file for reading.  For this example, assume that
    # the text file contains only ASCII characters.
    email_text = "The server has crashed.  Error below.  Check log \n"+repr(e)
    msg = MIMEText(email_text)

    me = 'eroubenoff@berkeley.edu'
    you = 'eroubenoff@berkeley.edu'
    msg['Subject'] = 'Server Error'
    msg['From'] = me
    msg['To'] = you

    # Send the message via our own SMTP server, but don't include the
    # envelope header.
    s = smtplib.SMTP('smtp.gmail.com')
    s.starttls()
    s.login(email_auth["un"], email_auth["pw"])
    s.sendmail(me, [you], msg.as_string())
    s.quit()


def error_email(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            send_error_email(e)
            return(e)
    return func_wrapper


