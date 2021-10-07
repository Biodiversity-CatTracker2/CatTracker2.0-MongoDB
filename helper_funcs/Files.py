import tempfile

import gridfs
import pandas as pd
from ssh_pymongo import MongoSession

from MongoDB_connect import mongodb_connect


class Files:
    def __init__(self):
        self.session = mongodb_connect()
        self.db = self.session.connection['cattracker2']
        self.fs = gridfs.GridFS(self.db)

    def add_file(self, collection, cat_name, filename, file_path):
        df = pd.read_csv(file_path)
        with tempfile.NamedTemporaryFile() as f:
            df.to_pickle(f.name)
            with open(f.name, 'rb') as pkl:
                self.db[collection].update_one(
                    {'cat': cat_name},
                    {'$set': {
                        'file': self.fs.put(pkl, filename=filename)
                    }})

    def load_file(self, collection, cat_name, **kwargs):
        return pd.read_pickle(
            self.fs.get(self.db[collection].find_one({'cat':
                                                      cat_name})['file']))


#-----------------------------------------------------------------------------
# Example:
# files = Files()
# kwargs = {
#     'collection': 'test',
#     'cat_name': 'kraken',
#     'filename': 'ACC_data.csv',
#     'file_path': '../CatTracker2.0/temp/acc_data_DataFR4.csv'
# }
# files.add_file(**kwargs)
# data = files.load_file(**kwargs)
# files.session.stop()
#-----------------------------------------------------------------------------
