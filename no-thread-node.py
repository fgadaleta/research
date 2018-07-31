# import random
import os
import networkx as nx
# import matplotlib.pyplot as plt
import logging
# import asyncio
# import threading
# import datetime
# import signal
# import sys
import time
import queue

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
MIN_NUM_SIG = 4


def create_transaction(nonce, value=None, signed_by=None):
    if not signed_by:
        signatures = []

    transaction = {'key': os.urandom(32).hex(),
                   'value': value,
                   'signed_by': signatures,
                   'nonce': nonce,
                   'prev_tx': '',
                   }
    return transaction


class Node():
    def __init__(self, label=None):
        self.storage = {}
        self.neighbors = []
        self.queue = []
        self.node_id = os.urandom(32).hex()
        self.node_label = label
        self.nonce = 0
        self.tx_queue = queue.Queue()
        self.tx_final = []
        self.promise = 0  # next sequence to validate

    def __repr__(self):
        return str(self.node_label)

    def connect(self, neighs: list):
        self.neighbors = neighs

    def sign(self, tx):
        # if havent signed yet, sign
        k, v, s, n = tx['key'], tx['value'], tx['signed_by'], tx['nonce']

        if self.node_id not in tx['signed_by']:
            s.append(self.node_id)

        return {'key': k, 'value': v, 'signed_by': s, 'nonce': n}

    def broadcast(self, tx):
        # send tx to neighbors
        for n in self.neighbors:
            time.sleep(0.01)
            log.debug('node %s broadcasting to %s', self.node_label, n)
            n.tx_queue.put(tx)

    def propose(self, value=None):
        self.nonce += 1
        tx = create_transaction(nonce=self.nonce, value=value)
        signed_tx = self.sign(tx)
        k, v, s, n = signed_tx['key'], signed_tx['value'], signed_tx['signed_by'], signed_tx['nonce']
        self._store(k, v, s, n)

        for n in self.neighbors:
            n.tx_queue.put(signed_tx)

    def _store(self, key, value, sigs, nonce):
        self.storage[key] = {'value': value, 'signed_by': sigs, 'nonce': nonce}

    def run(self):
        log.debug('processing queue in node %s', self.node_label)
        while not self.tx_queue.empty():
            # TODO add stochastic delay
            tx = self.tx_queue.get()
            log.debug('node %s reading queue tx=%s', self.node_label, tx)
            # extract key, value, signers
            k, v, s, n = tx['key'], tx['value'], tx['signed_by'], tx['nonce']
            tx_score = len(s)  # transaction score == num of signatures
            # this is a final transaction
            # WIP
            """
            if tx_score >= MIN_NUM_SIG:
                signed_tx = self.sign(tx)
                # promise to accept only the next
                self.promise = n+1
                self._store(k,v,s,n)
                continue
            """
            # tx is not in storage: sign and broadcast
            if k not in self.storage:
                signed_tx = self.sign(tx)
                # store signed transaction
                k, v, s, n = signed_tx['key'], signed_tx['value'], signed_tx['signed_by'], signed_tx['nonce']
                tx_score = len(s)
                self._store(k, v, s, n)
                # self.storage[k] = {'value': v, 'signed_by': s}
                time.sleep(.001)
                log.debug('node %s new transaction storing tx %s score=%s',
                          self.node_label, tx['key'], tx_score)
                # broadcast to network
                self.broadcast(signed_tx)

            # tx is already in local storage
            if k in self.storage:
                local_score = len(self.storage[k]['signed_by'])
                log.debug('node %s existing transaction %s', self.node_label, tx['key'])
                log.debug('\t local score = %s, new score = %s', local_score, tx_score)
                # accept only if better
                if tx_score > local_score:
                    log.debug('node %s better transaction tx %s', self.node_label, tx['key'])
                    # self.storage[k] = {'value': v, 'signed_by': s}
                    self._store(k, v, s, n)
                    time.sleep(.001)

                    # broadcast updated tx
                    new_tx = {'key': k, 'value': v, 'signed_by': s, 'nonce': n}
                    signed_tx = self.sign(new_tx)
                    self.broadcast(signed_tx)
            # self.tx_queue.task_done()


if __name__ == '__main__':
    NUM_NODES = 10
    MIN_SCORE = 4
    NUM_TXS = 30
    # TODO
    # transaction finality
    # leader election

    # setup network
    nodes = []
    for k, i in enumerate(range(NUM_NODES)):
        n = Node(label=k)
        nodes.append(n)
    network = nx.watts_strogatz_graph(NUM_NODES, 2, 0.1)
    # nx.draw(network, with_labels=True, font_weight='bold')
    # plt.show()

    # connect nodes as in graph
    for i in range(NUM_NODES):
        neigh_i = list(network[i].keys())
        # connect i to j
        connect_to = [nodes[j] for j in neigh_i]
        nodes[i].connect(connect_to)
        log.debug('node %s connected to %s', nodes[i], str(connect_to))

    # propose new transactions
    for i in range(NUM_TXS):
        nodes[0].propose()

    for i in range(NUM_NODES):
        nodes[i].run()

    for i in range(NUM_NODES):
        s = nodes[i].storage
        log.debug('node %s storage %s', i, s)

    # nodes[0].propose()
    # for i in range(num_nodes-1):
    #    j = i + 1
    #    assert(nodes[i].storage == nodes[j].storage)
