#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    with open(ratingsfilepath) as f:
	lines = f.readlines()
    for i in range(len(lines)):
	lines[i] = lines[i].split("::")
	lines[i] = lines[i][:3]
    cur.execute("""CREATE TABLE IF NOT EXISTS {0}(
		userid integer NOT NULL,
		movieid integer NOT NULL,
		rating real NOT NULL,
		PRIMARY KEY (userid, movieid, rating))""".format(ratingstablename))
    for line in lines:
	cur.execute("INSERT INTO {0} VALUES (%s,%s,%s)".format(ratingstablename), line)
    #cur.execute("SELECT * FROM {0} WHERE rating<3".format(ratingstablename))
    #p = cur.fetchall()
    #print(p)

    cur.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    r_range = 5/numberofpartitions
    for i in range(numberofpartitions):
	if i==0:
		cur.execute("""CREATE TABLE {0} AS
				SELECT *
				FROM {1}
				WHERE 0 <= rating AND rating <= {2}"""
				.format('range_part'+str(i),ratingstablename, (i+1)*r_range))
	else:
		cur.execute("""CREATE TABLE {0} AS
				SELECT *
				FROM {1}
				WHERE {2} < rating AND rating <= {3}"""
				.format('range_part'+str(i),ratingstablename, i*r_range,(i+1)*r_range))

    cur.close()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    for i in range(numberofpartitions):
	    cur.execute("""CREATE TABLE {0} AS
				SELECT *
				FROM (SELECT *, row_number() over () from {1}) AS temp
				WHERE row_number%{2}={3}"""
				.format('rrobin_part'+str(i),ratingstablename,numberofpartitions,i))
    cur.close()



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT* FROM {0}".format(ratingstablename))
    p = cur.fetchall()
    r_num = len(p)%5
    cur.execute("INSERT INTO {0}(userid, movieid, rating) VALUES({1},{2},{3})".format('rrobin_part'+str(r_num),userid, itemid, rating))
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    if rating <=1:
	r_num = 0
    elif 1< rating and rating<=2:
	r_num  =1
    elif 2< rating and rating<=3:
	r_num  =2
    elif 3< rating and rating<=4:
	r_num  =3
    elif 4< rating and rating<=5:
	r_num  =4
    cur.execute("INSERT INTO {0}(userid, movieid, rating) VALUES({1},{2},{3})".format('range_part'+str(r_num),userid, itemid, rating))
    cur.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
