#!/usr/bin/env python3

import smtplib
from email.mime.text import MIMEText


class Notifier():
    def __init__(self, server, port, username, password, sender, receiver):
        self._server = server
        self._port = port
        self._username = username
        self._password = password
        self._sender = sender
        self._receiver = receiver

    def send(self, title, message):
        if self._server is not None and self._port is not None \
           and self._username is not None and self._password is not None \
           and self._sender is not None and self._password is not None:
            msg = MIMEText(message)
            msg['Subject'] = title
            msg['From'] = self._sender
            msg['To'] = self._receiver

            with smtplib.SMTP(self._server, self._port) as server:
                server.starttls()
                server.login(self._username, self._password)
                server.sendmail(self._sender, [self._receiver], msg.as_string())
                print(f'Notification send to {self._receiver}')