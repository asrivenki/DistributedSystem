


import math


def rangepartition(ratingstablename, n, conn):
    try:
        #conn = psycopg2.connect("dbname='tempdb' user='postgres' host='localhost' password='arunvetri'")
        #print ("Connection is suucessful")
        cur = conn.cursor()
        for i in range(0,n):
            #print ("i ", i)
            query = "DROP TABLE IF EXISTS range_part{0}".format(i)
            cur.execute(query)
            query1 = "CREATE TABLE range_part{0}(userID Integer,MovieID Integer,Rating Float)".format(i)
            cur.execute(query1)

        cur.execute("""SELECT * from Ratings""")
        # print(" Row count ")
        rows = cur.fetchall()
        index = 0
        for row in rows:
            userId = row[0]
            movieId = row[1]
            ratings = row[2]

            table_number = math.ceil(ratings*n/5)
            if table_number == 0:
                table_number = 1;
            query = "INSERT INTO range_part{0} values({1},{2},{3})".format(table_number-1,int(userId), int(movieId), float(ratings))
            cur.execute(query)
            index = index +1

    except Exception as err:
        {

        }


def rangeinsert(ratingstablename, userid, itemid, rating, conn):
    try:
        cur = conn.cursor();
        query = "select * from aux"
        cur.execute(query)
        rows = cur.fetchall()
        n = 1
        for row in rows:
            value = int(row[0])
            n = int(row[1])

        table_number = math.ceil(rating * n / 5)
        if table_number == 0:
            table_number = 1;
        query = "INSERT INTO range_part{0} values({1},{2},{3})".format(table_number, int(userid), int(itemid),
                                                                       float(rating))
        cur.execute(query)

    except Exception as err:
        {

        }




def deletepartitionsandexit(conn):
    try:
        #conn = psycopg2.connect("dbname='tempdb' user='postgres' host='localhost' password='arunvetri'")
        #print("Connection is suucessful")
        cur = conn.cursor()
        query = "select * from aux"
        cur.execute(query)
        rows = cur.fetchall()
        n = 1
        for row in rows:
            value = int(row[0])
            n = int(row[1])

        for i in range(0, n):
            #print("i ", i)
            query = "DROP TABLE IF EXISTS range_part{0}".format(i+1)
            cur.execute(query)
            query = "DROP TABLE IF EXISTS rrobin_part{0}".format(i)
            cur.execute(query)
        query = "DROP TABLE IF EXISTS aux"
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()

    except Exception as err:
        {

        }




def roundrobinpartition(ratingstablename, n, conn):
    try:
        #conn = psycopg2.connect("dbname='tempdb' user='postgres' host='localhost' password='arunvetri'")
        #print ("Connection is suucessful")
        #print("Creating auxillary table")

        cur = conn.cursor()
        query = "DROP TABLE IF EXISTS aux"
        cur.execute(query)
        query = "CREATE TABLE aux(count Integer,noOfPartition Integer)"
        cur.execute(query)
        for i in range(0,n):
            #print ("i ", i)
            query = "DROP TABLE IF EXISTS rrobin_part{0}".format(i)
            cur.execute(query)
            query1 = "CREATE TABLE rrobin_part{0}(userID Integer,MovieID Integer,Rating Float)".format(i)
            cur.execute(query1)
        query = "SELECT * from {0} limit 1000".format(ratingstablename)
        cur.execute(query)
        # print(" Row count ")
        rows = cur.fetchall()
        index = 0
        for row in rows:
            userId = row[0]
            movieId = row[1]
            ratings = row[2]
            query = "INSERT INTO rrobin_part{0} values({1},{2},{3})".format(index%n,int(userId), int(movieId), float(ratings))
            cur.execute(query)
            index = index +1

        query = "INSERT INTO aux values({0},{1})".format(index,n)
        cur.execute(query)
        conn.commit()
    except Exception as err:
        {

        }




def roundrobininsert(ratingstablename, userid, itemid, rating, conn):
    try:
        cur = conn.cursor();
        query = "select * from aux"
        cur.execute(query)
        rows = cur.fetchall()
        n = 1
        for row in rows:
            value = int(row[0])
            n = int(row[1])
        index = value
       # print(" Pritning index ",index )
        query = "INSERT INTO rrobin_part{0} values({1},{2},{3})".format(index % n, int(userid), int(itemid),float(rating))
        cur.execute(query)
        index += 1
        query = "update aux set count = {0}".format(index)
        cur.execute(query)
        conn.commit()
    except Exception as err:
        {

        }




def create_table(tablename,conn):
    try:
        #conn = psycopg2.connect("dbname='tempdb' user='postgres' host='localhost' password='arunvetri'")
        #print("Create Table")
        query = "create table {0} (UserId Integer, MovieID Integer, Rating Float)".format(tablename)
        cur = conn.cursor()

        cur.execute(query)
        #conn.commit()
        #cur.close()
        #conn.close()

    except Exception as err:
        {

        }


def delete_table(ratingstablename,conn):
    try:
       # conn = psycopg2.connect("dbname='tempdb' user='postgres' host='localhost' password='arunvetri'")
        #print("start delete table")
        query = "DROP TABLE IF EXISTS {0}".format(ratingstablename)
        cur = conn.cursor()

        cur.execute(query)
        #conn.commit()
        #cur.close()
        #conn.close()
        #print("Complete delete table")
    except Exception as err:
        {

        }

def insert_table(filepath,conn,tablename):
    try:
        with open(filepath) as f:
            content = f.readlines()
    except:
        {

        }
    try:
        #conn = psycopg2.connect("dbname='tempdb' user='postgres' host='localhost' password='arunvetri'")
        #print("Connection is suucessful")
        cur = conn.cursor()
        for line in content:
            s = line.split("::")
            #print(s[0] , " ", s[1], " ", s[2])
            userId = int(s[0])
            movieId = int(s[1])
            ratings = float(s[2])
            query = "INSERT INTO ratings (userID,MovieID,Rating) VALUES ({0},{1},{2})".format(userId,movieId,ratings)
            data = (int(userId), int(movieId), float(ratings))
            cur.execute(query, data)
        #conn.commit()
        #cur.close()
        #conn.close()
    except Exception as err:
        {

        }

def loadratings(ratingstablename, filepath, openconnection):
     delete_table(ratingstablename,openconnection)
     create_table(ratingstablename,openconnection)
     insert_table(filepath, openconnection, ratingstablename)

