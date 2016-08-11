import msock.client
import logging
import time
logging.basicConfig(level=logging.DEBUG)
c = msock.client.Client()
c.connect('tcp://127.0.0.1:5678')
chan = c.create_channel(1)
time.sleep(1)
chan.send(b'hello world')
chan.close()
time.sleep(60)
