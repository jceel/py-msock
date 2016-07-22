import msock.client
import logging
import time
logging.basicConfig(level=logging.DEBUG)
c = msock.client.Client()
c.connect('tcp://127.0.0.1:5678')
chan = c.create_channel({})
print(c.channels)
chan.write("hello world")
time.sleep(60)
