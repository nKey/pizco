# -*- coding: utf-8 -*-

import gevent.monkey
gevent.monkey.patch_all()

import sys
import gevent

from pizco import Proxy
from pizco.geventagentmanager import GeventAgentManager

if sys.version_info < (3,0):
    input = raw_input


def on_lights_on(value, old_value):
    print('The lights_on has been changed from {0} to {1}'.format(old_value, value))


def on_door_open(value, old_value):
    print('The front door has been changed from {0} to {1}'.format(old_value, value))


def on_color_change(value):
    print('The color has been changed from unkown to {0}'.format(value))

proxy = Proxy('tcp://127.0.0.1:8000', manager=GeventAgentManager)

proxy.lights_on_changed.connect(on_lights_on)
proxy.door_open_changed.connect(on_door_open)
proxy.color_changed.connect(on_color_change)


def func():
    while True:
        gevent.sleep(1)

gevent.spawn(func).join()
