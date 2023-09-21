import sqlite3

def init():
    con = sqlite3.connect("data.db")
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


def _is_exists_exp(cur, expid):
    exp_exists = cur.execute(f"SELECT 1 from meta where name = '{expid}';").fetchone()
    return not (exp_exists is None)


def _is_exists_exp_file(cur, expid, filename):
    table_exists = cur.execute(f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{filename}';").fetchone()
    return not (table_exists is None)


def _iter_exp_names(cur):
    for row in cur.execute("SELECT name FROM meta"):
        yield row[0]


def _iter_exp_filenames(cur, expid):
    for row in cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name != 'meta' AND name != 'sqlite_sequence';"):
        yield row[0]

def _get_filedata(cur, expid, filename):
    data = cur.execute(f"SELECT data FROM {filename} WHERE expid IN (SELECT id FROM meta WHERE name = '{expid}');").fetchone()
    return data[0]


def _get_filedata_len(cur, expid, filename):
    row = cur.execute(f"SELECT length(data) FROM {filename} WHERE expid IN (SELECT id FROM meta WHERE name = '{expid}');").fetchone()
    return row[0]

def _clear_filedata(cur, expid, filename):
    return cur.execute(f"UPDATE aaaa SET data = ? WHERE expid IN (SELECT id FROM meta WHERE name = ?);", ("", expid))

def _set_filedata(cur, expid, filename, offset, blob):
    data = bytearray(_get_filedata(cur, expid, filename).encode())
    data[offset:] = blob
    return cur.execute(f"UPDATE aaaa SET data = ? WHERE expid IN (SELECT id FROM meta WHERE name = ?);", (data.decode(), expid))

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
    usage="""
Userspace hello example

""" + Fuse.fusage
    server = HelloFS("data.db", version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()

if __name__ == '__main__':
    main()

