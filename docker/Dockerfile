FROM docker.io/library/python:3.11-alpine
WORKDIR /app
COPY requirements.txt .
RUN apk add --no-cache --virtual .build-dep build-base pkgconfig fuse-dev &&\
    pip install -r requirements.txt &&\
    apk del .build-dep &&\
    apk add --no-cache fuse

COPY . /app/
ENV MOUNT_POINT /mnt/mnt
CMD sh -c 'trap "umount $MOUNT_POINT" SIGINT; python main.py $MOUNT_POINT && tail -f /dev/null'
