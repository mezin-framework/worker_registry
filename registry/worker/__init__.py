import traceback
import json
from redis import Redis
from threading import Thread
from hashlib import sha256
from datetime import datetime
from random import randint
from utils.work_distributer.requester import RefreshRequester
from time import sleep

from registry.db import db

class RefreshWorker(object):
    ''' Responsible for using a Academic Parser
        to fetch information from the portal
        and communicate to API.
     '''

    def __init__(self):
        self.queue = RefreshQueue()
        self.workers = []
        self.working = False
        self.ping = Ping(self.workers, self.working)
        self.ping.start()
        print(db.get('plugins'))

    def send_plugins(self, worker):

        def send_plugin(requester, worker, plugin):
            requester.block_request({
            "action": "install_plugin",
            "plugin_repo": plugin
            })

        plugins = db.get('plugins')
        plugins = [db.get(p) for p in plugins]
        requester = RefreshRequester(worker)
        for p in plugins:
            Thread(target=send_plugin, args=(requester, worker, p)).start()

    def run(self):
        while True:
            try:
                data = self.queue.get_new_payload()
                self.working = True
                action = data.get('action')
                if action == 'register':
                    worker = data.get('name')
                    if worker not in self.workers:
                        self.workers.append(worker)
                        self.queue.respond({"status": 'success'})
                        print("Added worker {}".format(worker),
                              "\n Now with {} workers registered".format(len(self.workers)))
                        self.send_plugins(worker)
                elif action == 'install_plugin':
                    plugins = db.get('plugins')
                    plugin_name = data.get('plugin_name')
                    plugin_repo = data.get('plugin_repo')
                    if plugin_name not in plugins:
                        db.put_plugin(plugin_name, plugin_repo)
                    threads = []
                    for queue in self.workers:
                        requester = RefreshRequester(queue)
                        t = Thread(target=requester.block_request, args=(data,))
                        threads.append(t)
                        t.start()
                    for t in threads:
                        t.join()
                    self.queue.respond({"status": "success"})
                elif action == 'get_plugins':
                    plugins = db.get('plugins')
                    plugin_repos = [{
                        "plugin_name": p,
                        "plugin_repo": db.get(p)
                    } for p in plugins]
                    self.queue.respond({"plugins": plugin_repos, "status": "success"})
                self.working = False
            except:
                traceback.print_exc()


class RefreshQueue(object):
    ''' Responsible for encapsulating Queue behaviour,
        such as getting a new payload.
    '''

    QUEUE = 'worker_registry'

    def __init__(self):
        self.redis = Redis(host='redis')
        self.work_id = ''

    def get_new_payload(self):
        key, value = self.redis.brpop(self.QUEUE)
        data = json.loads(value.decode())
        self.work_id = data.get('work_id')
        self.redis.lpush(self.work_id, json.dumps({"status": "processing"}))
        return data

    def respond(self, data):
        if self.work_id:
            self.redis.lpush(self.work_id, json.dumps(data))
            self.work_id = ''


class Ping(Thread):

    def __init__(self, workers, is_working):
        self.workers = workers
        self.is_working = is_working
        Thread.__init__(self)

    def run(self):

        def ping_worker(worker, not_responding):
            requester = RefreshRequester(worker, timeout=2)
            response = requester.block_request({
                "action": "ping"
            })
            if not response:
                not_responding.append(worker)

        while True:
            try:
                threads = []
                not_responding = []

                for worker in self.workers:
                    t = Thread(target=ping_worker, args=(worker,not_responding))
                    threads.append(t)
                    t.start()

                for t in threads:
                    t.join()

                if len(not_responding) > 0:
                    print("Found {} workers not responding, waiting for delete".format(len(not_responding)))

                    while self.is_working:
                        sleep(0.5)

                    for worker in not_responding:
                        self.workers.remove(worker)

                    print("Workers deleted")

                sleep(10)
            except:
                traceback.print_exc()
