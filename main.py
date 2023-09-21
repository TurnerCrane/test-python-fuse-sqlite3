import sqlite3


GROUPS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    UNIQUE(name)
)
"""


DATAS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS datas (
    data_id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER,
    name TEXT,
    data BLOB,
    UNIQUE(group_id, name),
    foreign key (group_id) references groups(group_id)
)
"""


class DB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.ensure_tables()

    def ensure_tables(self):
        with self.conn:
            self.conn.execute(GROUPS_TABLE_SQL)
            self.conn.execute(DATAS_TABLE_SQL)

    def is_exists_group(self, group_id):
        exists = self.conn.execute("SELECT 1 from groups WHERE name = ?;", (group_id,)).fetchone()
        return not (exists is None)

    def is_exists_data(self, group_id, data_name):
        exists = self.conn.execute("SELECT 1 FROM datas WHERE group_id = ? AND name = ?;", (group_id, data_name)).fetchone()
        return not (exists is None)
    
    def iter_group_names(self):
        for row in self.conn.execute("SELECT name FROM groups"):
            yield row[0]
    
    def iter_group_data_names(self, group_name):
        for row in self.conn.execute("SELECT name FROM datas WHERE group_id IN (SELECT group_id FROM groups WHERE name = ?)", (group_name,)):
            yield row[0]

    def _get_data_rowid(self, group_name, data_name):
        result = self.conn.execute("SELECT rowid FROM datas WHERE group_id IN (SELECT group_id FROM groups WHERE name = ?) AND name = ?;", (group_name, data_name)).fetchone()
        if not result:
            return
        return result[0]

    def get_data(self, group_name, data_name):
        rowid = self._get_data_rowid(group_name, data_name)
        if not rowid:
            return

        return self.conn.blobopen("datas", "data", rowid)
    
    def write_data(self, group_name, data_name, data, offset=0):
        with self.conn as conn:
            old = self.get_data(group_name, data_name).read()

            size = offset + len(data)
            conn.execute("UPDATE datas SET data = zeroblob(?) WHERE group_id IN (SELECT group_id FROM groups WHERE name = ?) AND name = ?;", (size, group_name, data_name))

            rowid = self._get_data_rowid(group_name, data_name)
            if not rowid:
                return

            with conn.blobopen("datas", "data", rowid) as blob:
                blob.write(old)
                blob[offset:] = data

    def create_group(self, group_name):
        with self.conn as conn:
            conn.execute("INSERT INTO groups(name) VALUES(?);", (group_name,))

    def create_data(self, group_name, data_name):
        with self.conn as conn:
            conn.execute("INSERT INTO datas(group_id, name, data) VALUES((SELECT group_id FROM groups WHERE name = ?), ?, zeroblob(0));", (group_name, data_name))


db = DB(":memory:")
db.create_group("test")
db.create_data("test", "test")
db.write_data("test", "test", b'{"a": 100')
db.write_data("test", "test", b', "b": 200}', 9)
print(list(db.iter_group_names()))
print(list(db.iter_group_data_names("test")))
print(db.get_data("test", "test").read())
print(db.conn.execute("SELECT json_extract(data, '$.b') FROM datas;").fetchall())
breakpoint()
"""


def _split_data_path(path):
    head, tail = os.path.split(path)
    if head == "/":
        if not tail:
            return None, None
        return tail, None

    head2, tail2 = os.path.split(head)
    if head2 == "/":
        if not tail2:
            return None, None
        return tail2, tail

    return None, None

import sqlite3

def init():
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS meta(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, UNIQUE(name))")
    cur.execute("CREATE TABLE IF NOT EXISTS filea(id INTEGER, col1 TEXT, col2 INTEGER)")
    con.close()

import os
import stat
import errno
import fuse
from fuse import Fuse
import os.path


fuse.fuse_python_api = (0, 2)


class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


class HelloFS(Fuse):
    def __init__(self, dbpath, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.con = sqlite3.connect("data.db", check_same_thread=(False if sqlite3.threadsafety == 3 else True))

    def getattr(self, path):
        expid, filename = _split_data_path(path)
        print(f"getattr: path={path}, expid={expid}, filename={filename}")

        st = MyStat()

        if path == '/':
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 2
            return st

        expid, filename = _split_data_path(path)
        if expid is None and filename is None:
            return -errno.ENOENT

        if expid is None:
            return -errno.ENOENT

        cur = self.con.cursor()

        if not _is_exists_exp(cur, expid):
            return -errno.ENOENT

        if filename is None:
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 1
            st.st_size = 1
            return st

        if not _is_exists_exp_file(cur, expid, filename):
            return -errno.ENOENT

        st.st_mode = stat.S_IFREG | 0o755
        st.st_nlink = 1
        st.st_size = _get_filedata_len(cur, expid, filename)
        return st

    def readdir(self, path, offset):
        expid, filename = _split_data_path(path)
        print(f"readdir: path={path}, expid={expid}, filename={filename}")

        cur = self.con.cursor()
        for r in  '.', '..':
            yield fuse.Direntry(r)

        if expid is None:
            for row in _iter_exp_names(cur):
                yield fuse.Direntry(row)
            return

        if filename is None:
            for row in _iter_exp_filenames(cur, expid):
                yield fuse.Direntry(row)


    def open(self, path, flags):
        expid, filename = _split_data_path(path)
        print(f"open: path={path}, flags={flags}, expid={expid}, filename={filename}")

        cur = self.con.cursor()
        if _is_exists_exp_file(cur, expid, filename):
            return


    def read(self, path, size, offset):
        expid, filename = _split_data_path(path)
        print(f"read: path={path}, size={size}, offset={offset}, expid={expid}, filename={filename}")

        cur = self.con.cursor()
        s = _get_filedata(cur, expid, filename)
        if s is None:
            return

        data = s.encode()
        datalen = len(data)

        if offset < datalen:
            if offset + size > datalen:
                size = datalen - offset
            buf = data[offset:offset+size]
        else:
            buf = b''
        print(buf)

        return buf

    def write(self, path, buf, offset):
        expid, filename = _split_data_path(path)
        print(f"write: path={path}, buf={len(buf)}, offset={offset}, expid={expid}, filename={filename}")

        cur = self.con.cursor()
        _set_filedata(cur, expid, filename, offset, buf)
        return len(buf)

    def truncate(self, path, size):
        expid, filename = _split_data_path(path)
        cur = self.con.cursor()
        _clear_filedata(cur, expid, filename)


def main():
    usage="Userspace hello example" + Fuse.fusage
    server = HelloFS("data.db", version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()

if __name__ == '__main__':
    main()
"""
