import socket
from datetime import datetime as dt

class Task(object):
    def __init__(self, name, item, db_conf):
        self.name = name
        self.item = item
        self.id = {"name" : self.name, "item" : self.item}
        self.conn = MongoClient(db_conf["url"])
        self.db = self.conn[db_conf["db"]]

        self.db.tasks.update(self.id, {"$set" : {"state" : "running",
                                                 "started" : dt.now(),
                                                 "host" : socket.gethostname()}}, upsert=True)

    def log(self, level, msg):
        ts = dt.now()
        self.db.tasks.update(self.id, {"$push" : {"logs" : { "t" : ts, "l" : level, "m" : msg}}})
        print("{} {} {}".format(ts, level.upper(), msg))

    def info(self, msg):
        self.log("info", msg)

    def debug(self, msg):
        self.log("debug", msg)

    def progress(self, count=None, text=None):
        s, u = [("progress.updated", dt.now())], []
        if count is not None:
            s += [("progress.count", count)]
        else:
            u += [("progress.count", "")]
        if text is not None:
            s += [("progress.text", text)]
        else:
            u += [("progress.text", "")]
        upd = {"$set" : dict(s)}
        if len(u) > 0:
            upd["$unset"] = dict(u)
        self.db.tasks.update(self.id, upd)

    def finished(self, **kwargs):
        upd = [("state", "finished"), ("finished", dt.now())]
        upd += [("meta.{}".format(k), v) for k, v in kwargs.items()]
        self.db.tasks.update(self.id, {"$set" : dict(upd), "$unset" : {"progress" : ""}})