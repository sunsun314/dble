#!/bin/sh

echo "dble init&start in docker"

sh /opt/dble/bin/dble start
sleep 20
mysql -P9066 -u man1 -h 127.0.0.1 -p654321 -c "create database @@dataNode ='dn$1-4'"
mysql -P9066 -u root -h 127.0.0.1 -p123456 -c "source /opt/dble/conf/testdb.sql"

echo "dble init finish"