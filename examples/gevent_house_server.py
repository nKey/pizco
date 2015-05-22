# -*- coding: utf-8 -*-

from pizco import Server
from pizco.geventagentmanager import GeventAgentManager

from common import House

s = Server(House(), 'tcp://127.0.0.1:8000', manager=GeventAgentManager)

s.serve_forever()
