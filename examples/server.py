import os
import msock.server
import logging


def on_connection(conn):
    chan = conn.create_channel(1)
    os.write(chan._master.fileno(), b'adfafafd')
    while True:
        msg = os.read(chan._master.fileno(), 100)
        if not msg:
            print('closed')
            break

        print(msg)

logging.basicConfig(level=logging.DEBUG)
s = msock.server.Server()
s.open('tcp://127.0.0.1:5678')
s.on_connection = on_connection
s.run()
