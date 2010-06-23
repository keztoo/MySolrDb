
from MySolrDb import MySolrDb

db = MySolrDb()
db.connect(ip_address='localhost', port=8983)
## TODO: Waaa - I want db = MySolrDb.connect() like the big boys. sergey, where r u when i need u?
cursor = db.cursor()

## TODO: if db exists delete it (cant right now as we have no delete)
try:
    res = cursor.execute("create database mydb")
except:
    print "Warning - database mydb already exists!"

res = cursor.execute("use mydb")
create_table_statement = "CREATE TABLE test_table (id int, name VARCHAR(32), ssn VARCHAR(32))"
res = cursor.execute(create_table_statement)

res = cursor.execute('show databases')
print "Result from show databases --->", res
res = cursor.execute('show tables')
print "Result from show tabless --->", res

# try various mysql like operations 
res = cursor.execute("insert into mydb.test_table (name, ssn) values ('bla', '123-456-7890')")
print "res from insert into mydb.test_table (name, ssn) values ('bla', '123-456-7890')", res    

res = cursor.execute("insert into mydb.test_table (name, ssn) values ('ken smith', '000-00-0000')")
print "res from insert into mydb.test_table (name, ssn) values ('ken smith', '000-00-0000')", res    

res = cursor.execute("insert into mydb.test_table (name, ssn) values ('Joe Blow', '000-00-0000')")
print "res from insert into mydb.test_table (name, ssn) values ('Joe Blow', '000-00-0000')", res    

'''
# this is like a select * from test_table diagnostic dump
solr_str = "fl=*&q=column_name:mydb_test_table_*"
res = solr_request("localhost:8983", solr_str)
print "res from select * done manually --->", res
'''

select_str = "select * from test_table where name = 'ken smith'" 
print "\nselect ...", select_str
res = cursor.execute(select_str)
print "res is --->", res
res = cursor.fetchall()
print "docs --->", res

select_str = "select * from test_table where name = 'ken smith' and ssn = '000-00-0000'" 
print "\nselect ...", select_str
res = cursor.execute(select_str)
print "res is --->", res
res = cursor.fetchall()
print "docs --->", res

select_str = "select ssn from test_table where name = 'ken smith' and ssn = '000-00-0000'" 
print "\nselect ...", select_str
res = cursor.execute(select_str)
print "res is --->", res
res = cursor.fetchall()
print "docs --->", res

select_str = "select name, ssn from test_table where name = 'ken smith' and ssn = '000-00-0000'" 
print "\nselect ...", select_str
res = cursor.execute(select_str)
print "res is --->", res
res = cursor.fetchall()
print "docs --->", res



