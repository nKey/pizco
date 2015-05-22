import gevent
import gevent.pool
import zmq.green as zmq


class GeventIOLoop(object):

    def __init__(self, num_handlers=3):
        self.running = False
        self.pool = gevent.pool.Pool()
        self.handler_pool = gevent.pool.Pool(size=num_handlers)

    def add_callback(self, callback, *args, **kwargs):
        return self.pool.spawn(callback, *args, **kwargs)

    def add_timeout(self, deadline, callback):
        return self.pool.spawn_later(deadline, callback)

    def join(self):
        self.pool.join()
        self.handler_pool.join()


class GeventZMQStream(object):

    def __init__(self, socket, io_loop):
        self.loop = io_loop
        self.socket = socket
        self.setsockopt = self.socket.setsockopt

    def on_recv_stream(self, callback):
        return self.loop.add_callback(self._loop(self.socket, callback))

    def _loop(self, stream, handler):
        def message_loop():
            while self.loop.running:
                if self.socket.closed:
                    break
                try:
                    message = stream.recv_multipart()
                    self.loop.handler_pool.spawn(handler, stream, message)
                except zmq.ZMQError:
                    pass
                finally:
                    gevent.sleep(0)
        return message_loop

    def closed(self):
        return self.socket.closed

    def __getattr__(self, name):
        if hasattr(self.socket, name):
            return getattr(self.socket, name)
        raise AttributeError


class GeventAgentManager(object):

    zmq = zmq

    @classmethod
    def add(cls, agent):
        agent.loop.running = True

    @classmethod
    def remove(cls, agent):
        agent.loop.running = False

    @classmethod
    def is_loop_in_current_thread(cls, loop):
        return True

    @classmethod
    def join(cls, loop, agent):
        loop.join()

    @classmethod
    def loop(self):
        return GeventIOLoop()

    @classmethod
    def stream(self, socket, io_loop):
        return GeventZMQStream(socket, io_loop)

    @classmethod
    def event(self):
        return gevent.event.Event()

    @classmethod
    def lock(self): # TODO create gevent like lock
        pass
