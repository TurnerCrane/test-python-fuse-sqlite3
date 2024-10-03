import errno
import logging
import os
import stat
import time

import fuse
from fuse import Fuse

from db import DB


fuse.fuse_python_api = (0, 2)
logging.basicConfig(filename='/app/app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Stat(fuse.Stat):
    def __init__(self, **kwargs):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = time.time()
        self.st_mtime = time.time()
        self.st_ctime = time.time()
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
        logging.info(f"getattr: path={path}, group={group_name}, data={data_name}")

        # request '/' attr
        if group_name is None and data_name is None:
            return Stat(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)

        # request '/<group_name>'
        elif group_name is not None and data_name is None:
            if not self.db.is_exists_group(group_name):
                return -errno.ENOENT
            return Stat(st_mode=(stat.S_IFDIR | 0o755), st_nlink=1)

        # request '/<group_name>/<data_name>'
        elif group_name is not None and data_name is not None:
            if not self.db.is_exists_data(group_name, data_name):
                logging.debug(f"getattr: {group_name}, {data_name} not exist")
                return -errno.ENOENT

            row = self.db.get_data_owner(group_name, data_name)
            # row = self.db.conn.execute("SELECT gid, uid FROM datas WHERE group_id IN (SELECT group_id FROM groups WHERE name = ?) AND name = ?;",(group_name, data_name)).fetchone()
            # loggin.debug(f"getattr: row={row}")

            if row:
                gid, uid = row
            else:
                return -errno.ENOENT
            logging.debug(f"getattr: gid={gid}, uid={uid}")

            size = len(self.db.get_data(group_name, data_name))
            mode = self.db.get_data_mode(group_name, data_name)

            timestamp = self.db.get_timestamps(group_name, data_name)
            if timestamp:
                atime, mtime, ctime = timestamp
            else:
                atime, mtime, ctime = (None, None, None)
            # atime, mtime, ctime = self.db.get_timestamps(group_name, data_name)
            logging.debug(f"getattr: atime={atime}, mtime={mtime}, ctime={ctime}")

            # FIXME(なんか複雑)
            if atime is None or mtime is None or ctime is None:
                now = time.time()
                self.db.update_timestamps(group_name, data_name, atime=now, mtime=now, ctime=now)
                return Stat(st_mode=(stat.S_IFREG | mode), st_nlink=1, st_size=size, st_gid=gid, st_uid=uid, st_atime=now, st_mtime=now, st_ctime=now)
            else:
                return Stat(st_mode=(stat.S_IFREG | mode), st_nlink=1, st_size=size, st_gid=gid, st_uid=uid, st_atime=atime, st_mtime=mtime, st_ctime=ctime)
        else:
            return -errno.ENOENT

    def readdir(self, path, offset):
        group_name, data_name = _split_data_path(path)
        logging.info(f"readdir: path={path}, offset={offset}, group={group_name}, data={data_name}")

        for r in ['.', '..']:
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
        logging.info(f"open: path={path}, flags={flags}, group={group_name}, data={data_name}")

        if group_name and not self.db.is_exists_group(group_name):
            return -errno.ENOENT

        if data_name and not self.db.is_exists_data(group_name, data_name):
            return -errno.ENOENT

    def read(self, path, size, offset):
        group_name, data_name = _split_data_path(path)
        logging.info(f"read: path={path}, size={size}, offset={offset}, group={group_name}, data={data_name}")

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
        logging.info(f"write: path={path}, buf=({len(buf)}), offset={offset}, group={group_name}, data={data_name}")

        self.db.write_data(group_name, data_name, buf, offset=offset)

        now = time.time()
        self.db.update_timestamps(group_name, data_name, mtime=now)

        return len(buf)

    def truncate(self, path, size):
        group_name, data_name = _split_data_path(path)
        logging.info(f"truncate: path={path}, size={size}, group={group_name}, data={data_name}")
        # FIXME(viで変更するとファイルサイズがおかしい)
        # self.db.clear_data(group_name, data_name)

    def create(self, path, flags, mode):
        group_name, data_name = _split_data_path(path)
        logging.info(f"create: path={path}, flags={flags}, mode={mode}, group={group_name}, data={data_name}")

        self.db.create_data(group_name, data_name)

        now = time.time()
        self.db.update_timestamps(group_name, data_name, atime=now, mtime=now, ctime=now)

    def mkdir(self, path, mode):
        group_name, data_name = _split_data_path(path)
        logging.info(f"mkdir: path={path}, mode={mode}, group={group_name}, data={data_name}")

        self.db.create_group(group_name)

    def rmdir(self, path):
        group_name, _ = _split_data_path(path)
        logging.info(f"rmdir: path={path}, group={group_name}")
        self.db.delete_group(group_name)

    def unlink(self, path):
        group_name, data_name = _split_data_path(path)
        logging.info(f"unlink: path={path}, group={group_name}, data={data_name}")
        self.db.delete_data(group_name, data_name)

    def chmod(self, path, mode):
        group_name, data_name = _split_data_path(path)
        logging.info(f"chmod: path={path}, mode={mode}, group={group_name}, data={data_name}")

        if data_name:
            self.db.set_data_permissions(group_name, data_name, mode)
            self.db.update_timestamps(group_name, data_name, ctime=time.time())
            logging.debug(f"chmod: permissions changed for {data_name} in group {group_name} to {mode}")
        else:
            logging.warning(f"chmod: permission change for directory {group_name} is not allowed")
            return -errno.EPERM

    def chown(self, path, gid, uid):
        group_name, data_name = _split_data_path(path)
        logging.info(f"chown: path={path}, gid={gid}, uid={uid}, group={group_name}, data={data_name}")

        if data_name and self.db.is_exists_data(group_name, data_name):
            # if not self.db.set_data_owner(group_name, data_name, gid, uid):
            #     return -errno.ENOENT
            self.db.set_data_owner(group_name, data_name, gid, uid)
            self.db.update_timestamps(group_name, data_name, ctime=time.time())
            logging.info(f"chown: Owners changed for {data_name} in group {group_name} to {gid}:{uid}")
        else:
            logging.error(f"chown: {path} does not exist")
            return -errno.EPERM


def main():
    usage = "DBFS" + Fuse.fusage
    server = DBFS("data.db",
                  version="%prog " + fuse.__version__,
                  usage=usage,
                  dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()


if __name__ == '__main__':
    main()
