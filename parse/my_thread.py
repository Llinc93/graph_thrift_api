import ctypes
import inspect
import threading


from model.ent_graph import Neo4jClient


class MyThread(threading.Thread):
    def __init__(self, names, level, relationship_filter, flag=True):
        super(MyThread, self).__init__()
        self.data = None
        self.flag = None
        self.names = names
        self.level = level
        self.relationship_filter = relationship_filter
        self.flag = flag

    def run(self) -> None:
        if self.flag:
            self.data, self.flag = Neo4jClient().get_ent_relevance_seek_graph(self.names, self.level, self.relationship_filter)
        else:
            self.data, self.flag = Neo4jClient().get_ent_relevance_seek_graph_v2(self.names, self.level, self.relationship_filter)
        return None

    def get(self):
        return self.data, self.flag

    def stop(self):
        raise ValueError


def run(names, level, relationship_filter, relationship_filter_short):
    task = MyThread(names, level, relationship_filter)
    task_short = MyThread(names, level, relationship_filter_short, flag=False)
    task.start()
    task_short.start()
    task.join(20)
    try:
        if task.isAlive():
            task.stop()
        else:
            task_data, task_flag = task.get()
    except Exception:
        task_data, task_flag = '', False

    task_short.join()
    task_short_data, task_short_flag = task_short.get()

    if not task_flag and not task_short_flag:
        return '', False
    if task_flag:
        return task_data, True
    elif task_short_flag:
        return task_short_data, False
    else:
        return '', False
