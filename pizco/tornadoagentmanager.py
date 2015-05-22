import weakref
import threading

import zmq
from zmq.eventloop import zmqstream, ioloop

from . import compat
from . import LOGGER

global default_io_loop
default_io_loop = ioloop.ZMQIOLoop.instance


class TornadoAgentManager(object):

    agents = weakref.WeakKeyDictionary()  # agents in loop will be deleted when there will no more be reference to the loop

    threads = weakref.WeakKeyDictionary()

    in_use = compat.WeakSet()

    zmq = zmq

    @classmethod
    def add(cls, agent):
        loop = agent.loop
        try:
            cls.agents[loop].add(agent)
        except KeyError:
            LOGGER.debug("starting the loop {}".format(id(loop)))
            t = threading.Thread(target=loop.start, name='ioloop-{0}'.format(id(loop)))
            cls.agents[loop] = set([agent, ])
            cls.threads[loop] = t
            cls.in_use.add(loop)
            t.daemon = True
            t.start()
            LOGGER.debug("loop started {}".format(id(loop)))

    @classmethod
    def remove(cls, agent):
        LOGGER.debug("removing agent {0} {1}".format(type(agent), agent.rep_endpoint))

        loop = agent.loop
        if agent in cls.agents[loop]:
            cls.agents[loop].remove(agent)
            delattr(agent, "loop")
        else:
            LOGGER.warning("removing an allready removed agent")
            return
        #no more loop and no more loop in cls.in_use... kind of strange
        #only remove create the removal of the element (_proxy_stop_me, _proxy_stop_server)

        if (cls.agents[loop]) and (not loop in cls.in_use) and (loop != ioloop.IOLoop.instance()):
            LOGGER.debug("removing the loop {}".format(id(loop)))
            cls.in_use.remove(loop)
            loop.add_callback(loop.close)
            if not cls.is_loop_in_current_thread(loop):
                LOGGER.debug("trying to join {0} of {1}".format(type(agent), agent.rep_endpoint))
                cls.join(loop)
            else:
                LOGGER.debug("warning join {0} of {1} in same thread/loop".format(type(agent), agent.rep_endpoint))

    @classmethod
    def is_loop_in_current_thread(cls, loop):
        if threading.current_thread().ident == cls.threads[loop].ident:
            LOGGER.warning("WARNING LOOP IN CURRENT THREAD")
            return True
        else:
            return False

    @classmethod
    def join(cls, loop, agent):
        try:
            ret = None
            while cls.threads[loop].isAlive():
                LOGGER.debug("trying to join {0} of {1}".format(type(agent), agent.rep_endpoint))
                ret = cls.threads[agent.loop].join(10)
                if ret is None:
                    LOGGER.warning("timeout on thread join {0} of {1}".format(type(agent), agent.rep_endpoint))
                    break
                else:
                    LOGGER.info("ended up with ret=%s", ret)
            LOGGER.debug("stopped {}".format(agent.rep_endpoint))
        except (KeyboardInterrupt, SystemExit):
            return

    @staticmethod
    def set_default_ioloop(mode="new"):
        global default_io_loop
        if mode == "new":
            default_io_loop = ioloop.ZMQIOLoop
        elif mode == "instance":
            default_io_loop = ioloop.ZMQIOLoop.instance
        else:
            assert(mode in ["new", "instance"])

    @classmethod
    def loop(self):
        return default_io_loop()

    @classmethod
    def stream(self, socket, io_loop):
        return zmqstream.ZMQStream(socket, io_loop)

    @classmethod
    def event(self):
        return threading.Event()

    @classmethod
    def lock(self):
        return threading.Lock()
