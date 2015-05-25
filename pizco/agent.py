# -*- coding: utf-8 -*-
"""
    pyzco.agent
    ~~~~~~~~~~~

    Implements an Agent class that communicates over ZeroMQ.

    :copyright: 2013 by Hernan E. Grecco, see AUTHORS for more details.
    :license: BSD, see LICENSE for more details.
"""


import os
from collections import defaultdict

from . import LOGGER
from .protocol import Protocol
from .util import bind


class Agent(object):
    """An object that can communicate via ZMQ to other Agents.

    Each agent has:
    - a REP socket to receive requests
    - one REQ per each Agent that it has to talk to (stored in self.connections)
    - one PUB to emit notifications
    - one SUB to subscribe to notifications

    Messages are parsed and formatted by the Protocol Class

    :param rep_endpoint: endpoint of the REP socket.
    :param pub_endpoint: endpoint of the PUB socket.
    :param ctx: ZMQ context. If None, the default context will be used.
    :param loop: ZMQ event loop. If None, the default loop will be used.
    :param protocol: Protocol to be used for the messages.
    """

    def __init__(self, rep_endpoint='tcp://127.0.0.1:0', pub_endpoint='tcp://127.0.0.1:0',
                 ctx=None, manager=None, protocol=None):
        self.manager = manager
        self._running_lock = self.manager.lock()
        self._running = self.manager.event()
        self._ending = self.manager.event()
        self._running.clear()
        self._ending.clear()

        #one loop per process
        self.ctx = ctx or self.manager.zmq.Context.instance()
        self.loop = self.manager.loop()

        self.protocol = protocol or Protocol(os.environ.get('PZC_KEY', ''),
                                             os.environ.get('PZC_SER', 'pickle'))

        LOGGER.debug('New agent at %s with context %s and loop %s',
                     rep_endpoint, self.ctx, self.loop)

        #: Connections to other agents (endpoint:REQ socket)
        self.connections = {}

        #: Incoming request sockets
        rep = self.ctx.socket(self.manager.zmq.REP)
        self.rep_endpoint = bind(rep, rep_endpoint)

        LOGGER.debug('Bound rep at %s REP', self.rep_endpoint)

        #: Subscribers per topics (topic:count of subscribers)
        self.subscribers = defaultdict(int)

        #: Outgoing notification socket
        pub = self.ctx.socket(self.manager.zmq.XPUB)
        self.pub_endpoint = bind(pub, pub_endpoint)

        LOGGER.debug('%s PUB: %s', self.rep_endpoint, self.pub_endpoint)

        #: Incoming notification socket
        sub = self.ctx.socket(self.manager.zmq.SUB)
        self.sub_endpoint = bind(sub)
        LOGGER.debug('%s SUB: %s', self.rep_endpoint, self.sub_endpoint)

        #: dict (sender, topic), callback(sender, topic, payload)
        self.notifications_callbacks = {}
        #: endpoints to which the socket is connected.
        self.sub_connections = set()

        self.rep_to_pub = {}

        #Transforms sockets into Streams in the loop, add callbacks and start loop if necessary.
        self._start(rep, pub, sub)

        #usually the Instanciaton of the Proxy is not in the same IOloop as the "parent creator"
        #check the running thread assert is not the same as the loop
        if not self.manager.is_loop_in_current_thread(self.loop):
            self._running.wait()
            LOGGER.info("Agent started")

    def _start(self, rep, pub, sub, in_callback=False):
        if not in_callback:
            self.manager.add(self)
            self.loop.add_callback(lambda: self._start(rep, pub, sub, True))
        else:
            self.manager.add(self)
            self.rep = self.manager.stream(rep, self.loop)
            self.pub = self.manager.stream(pub, self.loop)
            self.sub = self.manager.stream(sub, self.loop)
            self.rep.on_recv_stream(self._on_request)
            self.pub.on_recv_stream(self._on_incoming_xpub)
            self.sub.on_recv_stream(self._on_notification)
            self._running.set()
            LOGGER.info('Started agent %s', self.rep_endpoint)

    def close_stream(self, stream):
        if stream.closed():
            LOGGER.error("closing an allready closed stream")
            return
        else:
            LOGGER.debug("close stream {}".format(str(stream)))
        stream.on_recv(None)
        stream.flush()
        stream.close()

    def close_connections(self):
        for sock in self.connections.values():
            sock.close()
        self.connections = {}

    def stop(self):
        """Stop actor unsubscribing from all notification and closing the streams.
        """
        LOGGER.debug("starting stopping from loop")
        with self._running_lock:
            if not self._running.isSet():
                LOGGER.warning("calling stop on a stopping object")
                return
            else:
                self._running.clear()

        LOGGER.debug("stopping actor")
        #send __status__ signal to "proxy or connected servers
        #unsubscribe will hang
        #find a way to unsubscribe asynchronously

        if hasattr(self, 'did_instantiate'):
            self.loop.add_callback(self.clean_instance)

        self.loop.add_callback(self.close_connections)

        self.loop.add_callback(lambda: self.close_stream(self.rep))
        self.publish('__status__', 'stop')
        for (endpoint, topic) in list(self.notifications_callbacks.keys()):
            self.unsubscribe(endpoint, topic)

        self.loop.add_callback(lambda: self.close_stream(self.pub))

        self.loop.add_callback(lambda: self.close_stream(self.sub))

        self.loop.add_callback(lambda: LOGGER.info("loop empty"))
        self.loop.add_callback(self._ending.set)

        self.manager.remove(self)

        #LOGGER.info('Stopped agent %s in loop %s', self.rep_endpoint, self.loop)

    def wait_stop(self, timeout=None):
        self._ending.wait(timeout)

    def stopped(self):
        return self._ending.isSet()

    def wait_start(self, timeout=None):
        self._running.wait(timeout)

    def started(self):
        return self._running.isSet()

    def clean_instance(self):
        LOGGER.info('cleaning served object')
        if hasattr(self.served_object, "stop"):
            self.served_object.stop()
        del self.served_object

    def __del__(self):
        if self.started() and not self.loop is None:
            self.stop()

    def request_polled(self, recipient, content, timeout=5000):
        """Send a request to another agent and waits for the response.

        Messages have the following structure (sender name, message id, content)
        This methods is executed in the calling thread.

        :param recipient: endpoint of the recipient.
        :param content: content to be sent.
        :return: The response of recipient.
        """
        LOGGER.debug('%s -> %s: %s', self, recipient, content)
        try:
            req = self.connections[recipient]
        except KeyError:
            req = self.ctx.socket(self.manager.zmq.REQ)
            req.connect(recipient)
            self.connections[recipient] = req

        msgid = req.send_multipart(self.protocol.format(self.rep_endpoint, '', content, None))
        if req.poll(timeout):
            sender, topic, content, msgid = self.protocol.parse(req.recv_multipart(), recipient, msgid)
        else:
            LOGGER.warning('timeout on %s-%s', self.rep_endpoint, content)
            content = None
        return content

    def request(self, recipient, content):
        """Send a request to another agent and waits for the response.

        Messages have the following structure (sender name, message id, content)
        This methods is executed in the calling thread.

        :param recipient: endpoint of the recipient.
        :param content: content to be sent.
        :return: The response of recipient.
        """
        LOGGER.debug('%s -> %s: %s', self, recipient, content)
        try:
            req = self.connections[recipient]
        except KeyError:
            req = self.ctx.socket(self.manager.zmq.REQ)
            req.connect(recipient)
            self.connections[recipient] = req

        msgid = req.send_multipart(self.protocol.format(self.rep_endpoint, '', content, None))
        sender, topic, content, msgid = self.protocol.parse(req.recv_multipart(), recipient, msgid)
        return content

    def _on_request(self, stream, message):
        """Handles incoming requests from other agents, dispatch them to
        on_request and send the response back on the same stream.

        Messages have the following structure (sender name, message id, message)
        This methods is executed in the IOLoop thread.
        """
        try:
            sender, topic, content, msgid = self.protocol.parse(message)
        except Exception as ex:
            topic = ret = msgid = ''
            LOGGER.debug(ex)
        else:
            LOGGER.debug('%s <- %s: (%s) %s',
                         self.rep_endpoint, sender, msgid, content)
            ret = self.on_request(sender, topic, content, msgid)
            LOGGER.debug('Return value for %s: %s', msgid, ret)

        stream.send_multipart(self.protocol.format(self.rep_endpoint, topic, ret, msgid))

    def on_request(self, sender, topic, content, msgid):
        """Handles incoming request from other agents and return the response
        that should be sent to the source.

        Overload this method on your class to provide an specific behaviour.
        Call super to enable

        This methods is executed in the IOLoop thread.

        :param sender: name of the sender.
        :param topic: topic of the message.
        :param content: content of the message.
        :param msgid: unique id of the message.
        :return: message to be sent to the sender
        """
        if content == 'info':
            return {'rep_endpoint': self.rep_endpoint,
                    'pub_endpoint': self.pub_endpoint}
        elif content == 'stop':
            self.loop.add_timeout(.1, self.stop)
            return 'stopping'
        return content

    def _publish(self, topic, content):
        """Publish a message to the PUB socket.

        This methods must be executed in IOLoop thread.

        """
        #TODO check issues in loop after restarting not connected

        if not self.pub.closed():  # TODO : would try except be quicker
            self.pub.send_multipart(self.protocol.format(self.rep_endpoint, topic, content))
        else:
            if self._running.isSet():
                LOGGER.error('TRYING TO publish on a closed pub socket %s', self.rep_endpoint)
            else:
                LOGGER.warning('TRYING TO publish on a not running server %s', self.rep_endpoint)

    def publish(self, topic, content):
        """Thread safe publish of a message to the PUB socket.

        The full topic is built from concatenating endpoint + '+' + topic

        Messages have the following structure: topic <STOP> (sender, message id, content)
        This method is executed in the calling thread, the actual publishing is done the IOLoop.

        :param topic: topic of the message.
        :param content: content of the message.
        """
        #TODO : check for closed here or there?
        if not hasattr(self, "loop"):
            LOGGER.warning("trying to publish on a removed system ({}) have"
                           " you proxied the server from itself?".format(self.rep_endpoint))
            return
        self.loop.add_callback(lambda: self._publish(topic, content))

    def _on_incoming_xpub(self, stream, message):
        """Handles incoming message in the XPUB sockets, increments or decrements the subscribers
        per topic and dispatch to on_subscribe, on_unsubscribe
        Messages contain a byte indicating if a subscription or unsubscription and the topic.

        This methods is executed in the IOLoop thread.
        """
        try:
            message = message[0].decode("utf-8")
            action, full_topic = message[0] == '\x01', message[1:]
            protocol, source, topic = full_topic.split('+')
        except Exception as ex:
            LOGGER.debug('Invalid message from %s: %s\n%s', stream, message, ex)
            return

        LOGGER.debug('Incoming XPUB %s %s', action, topic)

        if action:
            self.subscribers[topic] += 1
            self.on_subscribe(topic, self.subscribers[topic])
        elif self.subscribers[topic] > 0:
            self.subscribers[topic] -= 1
            self.on_unsubscribe(topic, self.subscribers[topic])

    def on_subscribe(self, topic, count):
        """Callback for incoming subscriptions.

        This methods is executed in the IOLoop thread.

        :param topic: a string with the topic.
        :param count: number of subscribers
        """

    def on_unsubscribe(self, topic, count):
        """Callback for incoming unsubscriptions.

        This methods is executed in the IOLoop thread.

        :param topic: a string with the topic.
        :param count: number of subscribers
        """

    def _subscribe(self, endpoint, agentid_topic):
        """Subscribe to a topic at endpoint.

        This methods must be executed in IOLoop thread.
        """
        if endpoint not in self.sub_connections:
            LOGGER.debug('subscribing to %s', endpoint)
            self.sub.connect(endpoint)
            self.sub_connections.add(endpoint)
        self.sub.setsockopt(self.manager.zmq.SUBSCRIBE, agentid_topic)
        LOGGER.debug('Subscription sent to %s', agentid_topic)

    def _unsubscribe(self, endpoint, agentid_topic):
        """Unsubscribe to a topic at endpoint.

        This methods must be executed in IOLoop thread.
        """
        self.sub.setsockopt(self.manager.zmq.UNSUBSCRIBE, agentid_topic)
        LOGGER.debug('Unsubscription sent to %s', agentid_topic)

    def subscribe(self, rep_endpoint, topic, callback=None, pub_endpoint=None):
        """Thread safe subscribe to a topic at endpoint from another agent
        and assign a callback for the specific endpoint and topic.

        The full topic is built from concatenating PROTOCOL_HEADER + '+' + endpoint + '+' + topic

        Notice that Agent.subscribe_to_agent takes the rep_endpoint
        of the other agent.

        This method will be executed in main thread, the actual subscription is done the IOLoop.

        :param rep_endpoint: endpoint of an agent REP socket.
        :param topic: a string with the topic to subscribe.
        :param callback: a callable with the (sender, topic, content)
        :param pub_endpoint: endpoint of an agent PUB socket, if not given it will be queried.
        """
        LOGGER.debug((pub_endpoint, rep_endpoint, self.rep_to_pub))
        #fixing socket connections with wildcard binding

        pub_endpoint = pub_endpoint or self.rep_to_pub.get(rep_endpoint, None)

        if pub_endpoint is not None:
            if pub_endpoint.startswith("tcp://*"):
                defined_endpoint = rep_endpoint.replace("/", "").split(":")[1]
                pub_endpoint = pub_endpoint.replace("*", defined_endpoint)
                rep_endpoint = rep_endpoint.split(":")
                rep_endpoint[1] = "//*"
                rep_endpoint = ":".join(rep_endpoint)

        if pub_endpoint is None:
            ret = self.request(rep_endpoint, 'info')
            pub_endpoint = ret['pub_endpoint']
            self.rep_to_pub[rep_endpoint] = pub_endpoint
        elif rep_endpoint not in self.rep_to_pub:
            self.rep_to_pub[rep_endpoint] = pub_endpoint

        agentid_topic = self.protocol.format(rep_endpoint, topic, just_header=True)
        LOGGER.debug('Subscribing to %s with %s', agentid_topic, callback)
        self.loop.add_callback(lambda: self._subscribe(pub_endpoint, agentid_topic))
        self.notifications_callbacks[(rep_endpoint, topic)] = callback

    def unsubscribe(self, rep_endpoint, topic, pub_endpoint=None):
        """Thread safe unsubscribe to a topic at endpoint and assign a callback
        for the specific endpoint and topic.

        This method will be executed in main thread, the actual unsubscription is done the IOLoop.

        :param rep_endpoint: endpoint of an agent REP socket.
        :param topic: a string with the topic to subscribe.
        :param pub_endpoint: endpoint of an agent PUB socket, if not given it will be queried.
        """
        #fixing binding to all addresses with wildcards in Proxy

        pub_endpoint = pub_endpoint or self.rep_to_pub.get(rep_endpoint, None)

        if pub_endpoint is None:
            ret = self.request(rep_endpoint, 'info')
            pub_endpoint = ret['pub_endpoint']
            self.rep_to_pub[rep_endpoint] = pub_endpoint

        agentid_topic = self.protocol.format(rep_endpoint, topic, just_header=True)
        if (rep_endpoint, topic) in self.notifications_callbacks:
            LOGGER.debug('Unsubscribing to %s', agentid_topic)
            self.loop.add_callback(lambda: self._unsubscribe(pub_endpoint, agentid_topic))
            del self.notifications_callbacks[(rep_endpoint, topic)]
        else:
            LOGGER.warning('Unsubscribing from an not connected signal')

    def _on_notification(self, stream, message):
        """Handles incoming messages in the SUB socket dispatching to a callback if provided or
        to on_notification.

        This methods is executed in the IOLoop thread.
        """
        try:
            sender, topic, content, msgid = self.protocol.parse(message)
            LOGGER.debug("RECEIVE notification from %s (topic: %s) ", sender, topic)
        except:
            LOGGER.debug('Invalid message %s', message)
        else:
            #callback = self.notifications_callbacks[(sender, topic)]
            callback = self.notifications_callbacks.get((sender, topic), None)  # brett modification
            if callback:
                callback(sender, topic, content, msgid)
            else:
                self.on_notification(sender, topic, content, msgid)

    def on_notification(self, sender, topic, content, msgid):
        """Default notification callback for (sender, topic) in which a callback is not provided.

        Override this method to provide a custom behaviour.
        This methods is executed in the IOLoop thread.

        :param sender: sender of the notification.
        :param topic: topic of the notification.
        :param content: content of the notification.
        :param msgid: message id.
        """
        LOGGER.debug('Received notification: %s, %s, %s, %s', sender, topic, msgid, content)

    def join(self):
        self.wait_stop()
        #AgentManager.join(self)
