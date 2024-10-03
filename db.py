import sqlite3
import time


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
    gid INTEGER,
    uid INTEGER,
    mode INTEGER,
    data BLOB,
    atime REAL,
    mtime REAL,
    ctime REAL,
    UNIQUE(group_id, name),
    foreign key (group_id) references groups(group_id)
)
"""


class DB:
    def __init__(self, path):
        check_same_thread = (sqlite3.threadsafety != 3)
        self.conn = sqlite3.connect(path, check_same_thread=check_same_thread)
        self.ensure_tables()

    def ensure_tables(self):
        with self.conn:
            self.conn.execute(GROUPS_TABLE_SQL)
            self.conn.execute(DATAS_TABLE_SQL)

    def is_exists_group(self, group_name):
        exists = self.conn.execute(
            """
            SELECT 1 from groups WHERE name = ?;
            """, (group_name,)
        ).fetchone()
        return not (exists is None)

    def is_exists_data(self, group_name, data_name):
        exists = self.conn.execute(
            """
            SELECT 1 FROM datas WHERE group_id IN
            (SELECT group_id FROM groups WHERE name = ?) AND name = ?;
            """, (group_name, data_name)
        ).fetchone()
        return not (exists is None)

    def iter_group_names(self):
        for row in self.conn.execute("SELECT name FROM groups"):
            yield row[0]

    def iter_group_data_names(self, group_name):
        for row in self.conn.execute(
            """
            SELECT name FROM datas WHERE group_id IN
            (SELECT group_id FROM groups WHERE name = ?)
            """, (group_name,)
        ):
            yield row[0]

    def _get_data_rowid(self, group_name, data_name):
        result = self.conn.execute(
            """
            SELECT rowid FROM datas WHERE group_id IN
            (SELECT group_id FROM groups WHERE name = ?) AND name = ?;
            """, (group_name, data_name)
        ).fetchone()
        if not result:
            return
        return result[0]

    def get_data(self, group_name, data_name):
        rowid = self._get_data_rowid(group_name, data_name)
        if not rowid:
            return

        return self.conn.blobopen("datas", "data", rowid)

    def get_data_mode(self, group_name, data_name):
        rowid = self._get_data_rowid(group_name, data_name)
        if rowid is None:
            return None
        mode = self.conn.execute(
            """
            SELECT mode FROM datas WHERE rowid = ?
            """, (rowid,)
        ).fetchone()
        return mode[0] if mode else None

    def write_data_org(self, group_name, data_name, data, offset=0):
        with self.conn as conn:
            old = self.get_data(group_name, data_name).read()

            size = offset + len(data)
            self.clear_data(group_name, data_name, size)

            rowid = self._get_data_rowid(group_name, data_name)
            if not rowid:
                return

            with conn.blobopen("datas", "data", rowid) as blob:
                blob.write(old)
                blob[offset:] = data

    def write_data(self, group_name, data_name, data, offset=0):
        with self.conn as conn:
            # 現在のデータを取得
            old_data = self.get_data(group_name, data_name).read()
            # 新しいサイズを計算
            new_size = max(len(old_data), offset + len(data))
            self.clear_data(group_name, data_name, new_size)

            rowid = self._get_data_rowid(group_name, data_name)
            if not rowid:
                return

            # データを書き込む
            with conn.blobopen("datas", "data", rowid) as blob:
                blob.write(
                    old_data[:offset] + data + old_data[offset + len(data):]
                )

    def clear_data(self, group_name, data_name, size=0):
        with self.conn as conn:
            conn.execute(
                """
                UPDATE datas SET data = zeroblob(?) WHERE group_id IN
                (SELECT group_id FROM groups WHERE name = ?) AND name = ?;
                """, (size, group_name, data_name)
            )

    def create_group(self, group_name):
        with self.conn as conn:
            conn.execute(
                """
                INSERT INTO groups(name) VALUES(?);
                """, (group_name,)
            )

    def create_data(self, group_name, data_name, gid=0, uid=0, mode=0o644):
        now = time.time()
        with self.conn as conn:
            conn.execute(
                """
                INSERT INTO datas(
                    group_id, name, gid, uid, mode, atime, mtime, ctime, data
                )
                VALUES(
                    (SELECT group_id FROM groups WHERE name = ?),
                    ?, ?, ?, ?, ?, ?, ?, zeroblob(0)
                );
                """, (group_name, data_name, gid, uid, mode, now, now, now)
            )

    def set_data_permissions(self, group_name, data_name, mode):
        rowid = self._get_data_rowid(group_name, data_name)
        if rowid is None:
            return None

        with self.conn as conn:
            conn.execute(
                """
                UPDATE datas SET mode = ? WHERE rowid = ?
                """, (mode, rowid)
            )

    def get_data_owner(self, group_name, data_name):
        rowid = self._get_data_rowid(group_name, data_name)
        if rowid is None:
            return None

        row = self.conn.execute(
            """
            SELECT gid, uid FROM datas WHERE rowid = ?
            """, (rowid, )
        ).fetchone()
        return row if row else None

    def set_data_owner(self, group_name, data_name, uid, gid):
        rowid = self._get_data_rowid(group_name, data_name)
        if rowid is None:
            return None

        with self.conn as conn:
            conn.execute(
                """
                UPDATE datas SET uid = ?, gid = ? WHERE rowid = ?
                """, (uid, gid, rowid))

    def delete_group(self, group_name):
        with self.conn as conn:
            conn.execute(
                """
                DELETE FROM groups WHERE group_id IN
                (SELECT group_id FROM groups WHERE name = ?);
                """, (group_name,)
            )

    def delete_data(self, group_name, data_name):
        with self.conn as conn:
            conn.execute(
                """
                DELETE FROM datas WHERE group_id IN
                (SELECT group_id FROM groups WHERE name = ?) AND name = ?;
                """, (group_name, data_name)
            )

    def get_timestamps(self, group_name, data_name):
        rowid = self._get_data_rowid(group_name, data_name)
        if rowid is None:
            return None

        row = self.conn.execute(
            """
            SELECT atime, mtime, ctime FROM datas WHERE rowid = ?;
            """, (rowid,)
        ).fetchone()
        return row if row else None
        # if result:
        #     atime, mtime, ctime = result
        #     return atime, mtime, ctime
        # else:
        #     return None, None, None

    def update_timestamps(self, group_name, data_name, atime=None, mtime=None, ctime=None):
        rowid = self._get_data_rowid(group_name, data_name)
        if rowid is None:
            return None

        with self.conn as conn:
            if atime is not None:
                conn.execute(
                    """
                    UPDATE datas SET atime = ? WHERE rowid = ?
                    """, (atime, rowid)
                )
            if mtime is not None:
                conn.execute(
                    """
                    UPDATE datas SET mtime = ? WHERE rowid = ?
                    """, (mtime, rowid)
                )
            if ctime is not None:
                conn.execute(
                    """
                    UPDATE datas SET ctime = ? WHERE rowid = ?
                    """, (ctime, rowid)
                )


if __name__ == '__main__':
    db = DB(":memory:")
    db.create_group("test")
    db.create_data("test", "test")
    db.write_data("test", "test", b'{"a": 100')
    db.write_data("test", "test", b', "b": 200}', 9)
    print(list(db.iter_group_names()))
    print(list(db.iter_group_data_names("test")))
    print(db.get_data("test", "test").read())
    print(db.conn.execute(
            "SELECT json_extract(data, '$.b') FROM datas;"
            ).fetchall()
    )
    breakpoint()
