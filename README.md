### introduction
mysql-tracker fetch the binlog from mysql
then I will update the comfiguration service for mysql-tracker in the last versions
### to see
[1]: [design details](http://blog.csdn.net/hackerwin7/article/details/39896173)  
[2]: [to kafka design](http://blog.csdn.net/hackerwin7/article/details/42713271)  
### build
```
mvn clean install
cd target/
tar xxx.tar.gz

### edit conf
vim conf/tracker.properties (or simple.properties)
./bin/start.sh

### tail log file
tail -f logs/xxx.log
```
### real-time job tracker
HA and real-time job is lack of configuration, to see next version
