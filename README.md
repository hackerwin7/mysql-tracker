### introduction
mysql-tracker fetch the binlog from mysql and parse to a data format which mysql-parser can consume  and convert to a custom data-format       
### to see
[1]: [design details](http://blog.csdn.net/hackerwin7/article/details/39896173)  
[2]: [to kafka design](http://blog.csdn.net/hackerwin7/article/details/42713271)  
### build
```
mvn clean package
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

### relative projects
[mysql-parser](https://github.com/hackerwin7/mysql-parser) (a custom data format of consumer consume from the mysql-tracker)    
[mysql-binlog-tracker](https://github.com/hackerwin7/mysql-binlog-tracker) (a new recontruct project from tracker and parser, long-time developing......)   
