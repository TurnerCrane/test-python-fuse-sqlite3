import os
import stat
import errno
import fuse
from fuse import Fuse
import os.path
from db import DB


fuse.fuse_python_api = (0, 2)


class Stat(fuse.Stat):
    def __init__(self, **kwargs):
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
        for k, v in kwargs.items():
            setattr(self, k, v)


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


class DBFS(Fuse):
    def __init__(self, dbpath, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = DB(dbpath)

    def getattr(self, path):
        group_name, data_name = _split_data_path(path)
        print(f"getattr: path={path}, group={group_name}, data={data_name}")

        # request '/' attr
        if group_name is None and data_name is None:
            return Stat(st_mode=stat.S_IFDIR|0o755, st_nlink=2)

        # request '/<group_name>'
        elif group_name is not None and data_name is None:
            if not self.db.is_exists_group(group_name):
                return -errno.ENOENT
            return Stat(st_mode=stat.S_IFDIR|0o755, st_nlink=1)

        # request '/<group_name>/<data_name>'
        elif group_name is not None and data_name is not None:
            if not self.db.is_exists_data(group_name, data_name):
                print(group_name, data_name, "not exist")
                return -errno.ENOENT

            size = len(self.db.get_data(group_name, data_name))
            return Stat(st_mode=stat.S_IFREG|0o644, st_nlink=1, st_size=size)
        
        else:
            return -errno.ENOENT

    def readdir(self, path, offset):
        group_name, data_name = _split_data_path(path)
        print(f"readdir: path={path}, offset={offset}, group={group_name}, data={data_name}")

        for r in  '.', '..':
            yield fuse.Direntry(r)

        # request '/<group_name>'
        if group_name is None and data_name is None:
            for name in self.db.iter_group_names():
                yield fuse.Direntry(name)

        # request '/<group_name>/<data_name>'
        elif group_name is not None and data_name is None:
            for name in self.db.iter_group_data_names(group_name):
                yield fuse.Direntry(name)

    def open(self, path, flags):
        group_name, data_name = _split_data_path(path)
        print(f"open: path={path}, flags={flags}, group={group_name}, data={data_name}")

        if group_name and not self.db.is_exists_group(group_name):
            return -errno.ENOENT

        if data_name and not self.db.is_exists_data(group_name, data_name):
            return -errno.ENOENT


    def read(self, path, size, offset):
        group_name, data_name = _split_data_path(path)
        print(f"read: path={path}, size={size}, offset={offset}, group={group_name}, data={data_name}")

        data = self.db.get_data(group_name, data_name)
        datalen = len(data)

        if offset < datalen:
            if offset + size > datalen:
                size = datalen - offset
            buf = data[offset:offset+size]
        else:
            buf = b''

        return buf

    def write(self, path, buf, offset):
        group_name, data_name = _split_data_path(path)
        print(f"read: path={path}, buf=({len(buf)}), offset={offset}, group={group_name}, data={data_name}")

        self.db.write_data(group_name, data_name, buf, offset=offset)
        return len(buf)

    def truncate(self, path, size):
        group_name, data_name = _split_data_path(path)
        print(f"truncate: path={path}, size={size}, group={group_name}, data={data_name}")
        self.db.clear_data(group_name, data_name)

    def create(self, path, flags, mode):
        group_name, data_name = _split_data_path(path)
        print(f"create: path={path}, flags={flags}, mode={mode}, group={group_name}, data={data_name}")
        self.db.create_data(group_name, data_name)


def main():
    usage="DBFS" + Fuse.fusage
    server = DBFS("data.db", version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()

if __name__ == '__main__':
    main()
