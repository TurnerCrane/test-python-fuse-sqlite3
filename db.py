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
        self.conn = sqlite3.connect(path, check_same_thread=(False if sqlite3.threadsafety == 3 else True))
        self.ensure_tables()

    def ensure_tables(self):
        with self.conn:
            self.conn.execute(GROUPS_TABLE_SQL)
            self.conn.execute(DATAS_TABLE_SQL)

    def is_exists_group(self, group_name):
        exists = self.conn.execute("SELECT 1 from groups WHERE name = ?;", (group_name,)).fetchone()
        return not (exists is None)

    def is_exists_data(self, group_name, data_name):
        exists = self.conn.execute("SELECT 1 FROM datas WHERE group_id IN (SELECT group_id FROM groups WHERE name = ?) AND name = ?;", (group_name, data_name)).fetchone()
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
            self.clear_data(group_name, data_name, size)

            rowid = self._get_data_rowid(group_name, data_name)
            if not rowid:
                return

            with conn.blobopen("datas", "data", rowid) as blob:
                blob.write(old)
                blob[offset:] = data

    def clear_data(self, group_name, data_name, size=0):
        with self.conn as conn:
            conn.execute("UPDATE datas SET data = zeroblob(?) WHERE group_id IN (SELECT group_id FROM groups WHERE name = ?) AND name = ?;", (size, group_name, data_name))

    def create_group(self, group_name):
        with self.conn as conn:
            conn.execute("INSERT INTO groups(name) VALUES(?);", (group_name,))

    def create_data(self, group_name, data_name):
        with self.conn as conn:
            conn.execute("INSERT INTO datas(group_id, name, data) VALUES((SELECT group_id FROM groups WHERE name = ?), ?, zeroblob(0));", (group_name, data_name))

if __name__ == '__main__':
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
