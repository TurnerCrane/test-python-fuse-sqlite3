insert into groups(name) values('foo');
insert into groups(name) values('bar');
insert into groups(name) values('baz');
insert into datas(group_id, name, data) values(1, 'hoge', zeroblob(0));
insert into datas(group_id, name, data) values(1, 'fuga', zeroblob(0));
insert into datas(group_id, name, data) values(2, 'hello', zeroblob(0));
insert into datas(group_id, name, data) values(3, 'data', zeroblob(0));

