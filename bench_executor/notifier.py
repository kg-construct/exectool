#!/usr/bin/env python3

"""
This module holds the Notifier class which is responsible for notifying the
user remotely when the execution is finished, interrupted or failed.
"""

import smtplib
from email.mime.text import MIMEText
try:
    from bench_executor import Logger
except ModuleNotFoundError:
    from logger import Logger


class Notifier():
    """Notify user via e-mail when execution finished, or failed."""

    def __init__(self, server: str, port: int, username: str, password: str,
                 sender: str, receiver: str, directory: str, verbose: bool):
        """Creates an instance of the Notifier class.

        Parameters
        ----------
        server : str
            The SMTP server domain name.
        port : int
            The SMTP server port.
        username : str
            The SMTP server username for sending an e-mail.
        password : str
            The SMTP server password for sending an e-mail.
        sender : str
            The e-mailaddress of the sender.
        receiver : str
            The e-mailaddress of the receiver.
        directory : str
            The path to the directory where the logs must be stored.
        verbose : bool
            Enable verbose logs.
        """
        self._server = server
        self._port = port
        self._username = username
        self._password = password
        self._sender = sender
        self._receiver = receiver
        self._logger = Logger(__name__, directory, verbose)

    def send(self, title: str, message: str) -> bool:
        """Send a notification via e-mail with a title and message.

        Parameters
        ----------
        title : str
            E-mail's subject
        message : str
            E-mail's body

        Returns
        -------
        success : bool
            Whether sending the notification was successfull or not.
        """
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
                server.sendmail(self._sender, [self._receiver],
                                msg.as_string())
                self._logger.info(f'Notification send to {self._receiver}')
        else:
            self._logger.debug('Parameters are missing to send an e-mail '
                               'notification')
            return False

        return True
