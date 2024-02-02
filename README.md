# install
```
% git clone git@github.com:yaamai/test-python-fuse-sqlite3.git
Cloning into 'test-python-fuse-sqlite3'...
Warning: Identity file /home/aina/.ssh/id_rsa not accessible: No such file or directory.
remote: Enumerating objects: 34, done.
remote: Counting objects: 100% (34/34), done.
remote: Compressing objects: 100% (20/20), done.
remote: Total 34 (delta 10), reused 32 (delta 8), pack-reused 0
Receiving objects: 100% (34/34), 7.35 MiB | 4.15 MiB/s, done.
Resolving deltas: 100% (10/10), done.

% cd test-python-fuse-sqlite3
% python -m venv venv
% source venv/bin/activate
% pip install -r requirements.txt
Collecting fuse-python==1.0.5 (from -r requirements.txt (line 1))
  Using cached fuse_python-1.0.5-cp311-cp311-linux_x86_64.whl
  Installing collected packages: fuse-python
  Successfully installed fuse-python-1.0.5

  [notice] A new release of pip is available: 23.2.1 -> 23.3.2
  [notice] To update, run: pip install --upgrade pip
```

# dataload
```
% python -m testdata.large
loading 394 datas
```

# mount
```
% mkdir mnt
% python main.py mnt
% ls mnt/650c762701e2b2fd3a42e530
config.json
% cat mnt/650c762701e2b2fd3a42e530/config.json
{"_id": "650c762701e2b2fd3a42e530", "index": 1, "guid": "b5be1dc4-051e-4ceb-9343-392ebacf5d87", "isActive": false, "balance": "$2,309.94", "picture": "http://placehold.it/32x32", "age": 31, "eyeColor": "blue", "name": "Byers Blevins", "gender": "male", "company": "QABOOS", "email": "byersblevins@qaboos.com", "phone": "+1 (846) 549-3974", "address": "570 McKibben Street, Sardis, Montana, 2579", "about": "Mollit qui in anim ad amet ea ex. Mollit eiusmod reprehenderit fugiat ullamco labore ea ut pariatur ad incididunt velit sit. Est reprehenderit enim id velit. Elit dolor anim occaecat mollit aliqua aliqua sit quis dolore ipsum est dolore.\r\nEnim dolore officia deserunt aliqua mollit nisi. Qui id sunt duis commodo amet occaecat. Officia laboris elit id ad. Sit amet laborum velit eu sit occaecat labore eiusmod ut anim occaecat. Et ea voluptate aliquip mollit reprehenderit do veniam ad quis consequat sunt Lorem officia. Eiusmod aute eiusmod culpa laboris anim cupidatat aliquip velit Lorem aliquip commodo in tempor ipsum. Pariatur culpa officia exercitation sit ullamco esse dolor officia.\r\nEnim pariatur id laboris in incididunt laboris qui excepteur consectetur do adipisicing et aute. Sit irure dolor incididunt aute eu. Irure nulla voluptate nostrud ullamco cillum. Exercitation consequat irure labore consequat in anim enim ipsum labore pariatur elit. Consequat ex laborum adipisicing labore consectetur sunt irure.\r\nExercitation proident ea laboris id. Nisi adipisicing consectetur non sit amet mollit nulla incididunt nisi nisi officia est cupidatat. Do laboris sint pariatur do sunt aliqua irure proident incididunt proident.\r\nConsequat ullamco nostrud dolor laborum cupidatat aliqua. Et elit eiusmod ullamco consequat qui nulla sunt quis. Et ex cupidatat nisi sit veniam. Consectetur non aute laboris nisi exercitation tempor anim cupidatat cillum ea cupidatat reprehenderit nulla. Enim ullamco ut enim ut pariatur do officia enim anim sunt magna sit Lorem Lorem.\r\nDuis proident amet occaecat deserunt sint. Deserunt do proident consectetur veniam deserunt occaecat magna cupidatat enim pariatur velit. Laboris mollit non irure velit Lorem laborum et ad quis nostrud duis cupidatat ea. Enim consectetur eiusmod tempor laborum sunt.\r\nAnim reprehenderit ea in ullamco ex adipisicing. Magna cupidatat non labore laborum velit tempor culpa deserunt et magna ad reprehenderit. Exercitation velit quis velit officia amet deserunt irure anim laborum. Irure sunt nostrud pariatur est officia ad occaecat et duis ipsum sunt in. Sint ullamco id amet est commodo in eiusmod consectetur cupidatat. Est do minim proident exercitation exercitation enim nisi quis esse.\r\nNulla voluptate anim laborum dolor quis eu labore excepteur ex magna id id tempor ex. Nulla nostrud tempor ad ipsum occaecat duis occaecat nisi amet ullamco nulla consectetur. Ea adipisicing aliqua pariatur exercitation cillum in fugiat cupidatat veniam deserunt in.\r\n", "registered": "2023-03-14T02:35:35 -09:00", "latitude": -32.562446, "longitude": 86.59273, "tags": ["dolore", "consectetur", "magna", "mollit", "qui", "minim", "occaecat"], "friends": [{"id": 0, "name": "Chase Berg"}, {"id": 1, "name": "Morgan Pate"}, {"id": 2, "name": "Angelia Fuentes"}], "greeting": "Hello, Byers Blevins! You have 8 unread messages.", "favoriteFruit": "apple"}%
```
