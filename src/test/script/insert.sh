#/bin/bash
echo $1
insert_sql="insert into test (name) values"
for i in $(seq 1 $1)
do
    insert_sql="${insert_sql}""('1'),"
    sleep $2
done
mysql -ucanal -pcanal -Dcanal_test -e "${insert_sql%,}"
