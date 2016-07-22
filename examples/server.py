import msock.server
import logging

logging.basicConfig(level=logging.DEBUG)
s = msock.server.Server()
s.open('tcp://127.0.0.1:5678')
s.run()

