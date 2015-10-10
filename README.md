### introduction
consumer for mysql-tracker, some operations to restructure thr format of message
mysql-parser fetch the messages from mysql-tracker and pacakge the new format messages
then I will update the comfiguration service for mysql-parser in the last versions
### to see
http://blog.csdn.net/hackerwin7/article/details/39896173
http://blog.csdn.net/hackerwin7/article/details/42713271
### build
```
mvn clean install
cd target/
tar xxx.tar.gz

### edit conf
vim conf/parser.properties
./bin/start.sh

### tail log file
tail -f logs/xxx.log
```
### real-time job tracker
HA and real-time job is lack of configuration, to see next version

