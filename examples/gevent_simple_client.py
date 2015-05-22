# -*- coding: utf-8 -*-

import sys
import time
import random
import gevent

if sys.version_info < (3,0):
    input = raw_input

from pizco.clientserver import Proxy
from pizco.geventagentmanager import GeventAgentManager

proxy = Proxy('tcp://127.0.0.1:8000', manager=GeventAgentManager)

colors = ('green', 'blue', 'white', 'yellow')


def func():
    while True:
        proxy.door_open = True
        proxy.lights_on = True
        time.sleep(.1)
        proxy.paint(random.choice(colors))
        proxy.lights_on = False
        proxy.door_open = False
        gevent.sleep(1)

gevent.spawn(func).join()
