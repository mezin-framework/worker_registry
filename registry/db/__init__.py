import os
import pickledb

def check_db_directory():
    exists = os.path.exists('data/')
    if not exists:
        os.mkdir('data')


class PluginDatabase(object):

    def __init__(self):
        self.db = pickledb.load('data/plugins.db', True)
        plugins = self.db.get('plugins')
        if type(plugins) is not list:
            self.db.lcreate('plugins')
    
    def get(self, key):
        return self.db.get(key)
    
    def put_plugin(self, plugin_name, plugin_repo):
        self.db.get('plugins').append(plugin_name)
        self.db.set(plugin_name, plugin_repo)

check_db_directory()
db = PluginDatabase()