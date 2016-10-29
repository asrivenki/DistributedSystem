#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################

tuple0 = []
tuple1 = []
tuple2 = []
tuple3 = []
tuple4 = []
merged = []


def merge_insert(openconnection, input_table, output_table):
    '''print tuple0
    print tuple1
    print tuple2
    print tuple3
    print tuple4
    print merged'''
    cursor = openconnection.cursor()
    if input_table == 'ratings':
        cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID INT, MovieID INT, Rating REAL)" % (output_table))
        for row in merged:
            for inner_row in row:
                cursor.execute('INSERT INTO %s(UserID, MovieID, Rating) VALUES(%s, %s, %s)' % (output_table, inner_row[0], inner_row[1], inner_row[2]))
    else:
        cursor.execute("CREATE TABLE IF NOT EXISTS %s(MovieID1 INT, Title VARCHAR(100),  Genre VARCHAR(100))" % (output_table))
        for row in merged:
            for inner_row in row:
                cursor.execute('INSERT INTO %s(MovieID1, Title, Genre) VALUES(\'%s\', \'%s\', \'%s\')' % (output_table, inner_row[0], inner_row[1], inner_row[2]))
    openconnection.commit()

def rangePartition(ratingstablename, column_name, openconnection):  # Reusing Range Partition

    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" % ratingstablename)
        if not bool(cursor.rowcount):
            print "Please Load Ratings Table first!!!"
            return
        #Finding the number of records in each partition.
        print 'Finding max'
        cursor.execute("SELECT max(%s) from %s " % (column_name, ratingstablename))
        max1 = 0
        row_max = cursor.fetchall()
        for row in row_max:
            #global max1
            max1 = row[0]

        print 'Finding min'
        cursor.execute("SELECT min(%s) from %s " % (column_name, ratingstablename))
        min1 = 0
        row_min = cursor.fetchall()
        for row in row_min:
            #global min1
            min1 = row[0]

        range1 = max1-min1
        print 'range ', range1
        threads = []
        for i in range(5):
            t = threading.Thread(target=worker, args=(i, ratingstablename, openconnection, range1, column_name, min1))
            threads.append(t)
            t.start()
        for thread in threads:
            thread.join()

        print "tuple0 ", len(tuple0)
        print "tuple1 ", len(tuple1)
        print "tuple2 ", len(tuple2)
        print "tuple3 ", len(tuple3)
        print "tuple4 ", len(tuple4)

        global merged;
        merged = [tuple0, tuple1, tuple2, tuple3, tuple4]

        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


def worker(i, ratingstablename, openconnection, range, column, min1):
    """thread worker function"""
    cursor = openconnection.cursor()
    each_range = float(range)/5.0
    print 'each range ', each_range
    print 'Worker: %s' % i
    if i == 0:
        start = min1 + 0.0
        end = min1 + each_range
        print 'start ', start
        print 'end ', end

        cursor.execute("SELECT * FROM %s WHERE %s >= %f AND %s <= %f order by %s" % (ratingstablename, column, start, column, end, column))
        global tuple0;
        tuple0 = cursor.fetchall()

    elif i == 1:
        start = min1 + each_range
        end = min1 + (2*each_range)
        print 'start ', start
        print 'end ', end
        cursor.execute("SELECT * FROM %s WHERE %s > %f AND %s <= %f order by %s" % (ratingstablename, column, start, column, end, column))
        global tuple1
        tuple1 = cursor.fetchall()

    elif i == 2:
        start = min1 + (2 * each_range)
        end = min1 + (3 * each_range)
        print 'start ', start
        print 'end ', end
        cursor.execute("SELECT * FROM %s WHERE %s > %f AND %s <= %f order by %s" % (
        ratingstablename, column, start, column, end, column))
        global tuple2
        tuple2 = cursor.fetchall()

    elif i == 3:
        start = min1 + (3 * each_range)
        end = min1 + (4 * each_range)
        print 'start ', start
        print 'end ', end
        cursor.execute("SELECT * FROM %s WHERE %s > %f AND %s <= %f order by %s" % (
        ratingstablename, column, start, column, end, column))
        global tuple3
        tuple3 = cursor.fetchall()

    elif i == 4:
        start = min1 + (4 * each_range)
        end = min1 + (5 * each_range)
        print 'start ', start
        print 'end ', end
        cursor.execute("SELECT * FROM %s WHERE %s > %f AND %s <= %f order by %s" % (
        ratingstablename, column, start, column, end, column))
        global tuple4
        tuple4 = cursor.fetchall()

    return


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    rangePartition(InputTable, SortingColumnName, openconnection)
    merge_insert(openconnection, InputTable, OutputTable)


#Parallel Join

def merge_insert_join(openconnection, input_table1, output_table):
    '''print tuple0
    print tuple1
    print tuple2
    print tuple3
    print tuple4
    print merged'''
    cursor = openconnection.cursor()
    if input_table1 == 'ratings':
        cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID INT, MovieID INT, Rating REAL, movieid1 int, title VARCHAR(200), genre VARCHAR(200))" % (output_table))
        for row in merged:
            for inner_row in row:
                cursor.execute('INSERT INTO %s(UserID, MovieID, Rating, movieid1, title, genre) VALUES(%s, %s, %s, %s, \'%s\', \'%s\')' % (output_table, inner_row[0], inner_row[1], inner_row[2], inner_row[3], inner_row[4], inner_row[5]))
    else:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS %s(movieid1 int, title VARCHAR(200), genre VARCHAR(200), UserID INT, MovieID INT, Rating REAL)" % (
            output_table))
        for row in merged:
            for inner_row in row:
                cursor.execute(
                    'INSERT INTO %s( movieid1, title, genre, UserID, MovieID, Rating) VALUES(%s, \'%s\', \'%s\', %s, %s, %s)' % (
                    output_table, inner_row[0], inner_row[1], inner_row[2], inner_row[3], inner_row[4], inner_row[5]))
    openconnection.commit()

def rangePartition_join(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):  # Reusing Range Partition

    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" % InputTable1)
        if not bool(cursor.rowcount):
            print "Please Load Ratings Table first!!!"
            return
        cursor.execute("select * from information_schema.tables where table_name='%s'" % InputTable2)
        if not bool(cursor.rowcount):
            print "Please Load Movies Table first!!!"
            return

        #Finding the number of records in each partition.
        print 'Finding max'
        cursor.execute("SELECT max(%s) from %s " % (Table2JoinColumn, InputTable2))
        max1 = 0
        row_max = cursor.fetchall()
        for row in row_max:
            #global max1
            max1 = row[0]

        print 'Finding min'
        cursor.execute("SELECT min(%s) from %s " % (Table2JoinColumn, InputTable2))
        min1 = 0
        row_min = cursor.fetchall()
        for row in row_min:
            #global min1
            min1 = row[0]

        range1 = max1-min1
        print 'range ', range1
        threads = []
        for i in range(5):
            t = threading.Thread(target=worker_join, args=(i, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection, range1, min1))
            threads.append(t)
            t.start()
        for thread in threads:
            thread.join()

        print "tuple0 ", len(tuple0)
        print "tuple1 ", len(tuple1)
        print "tuple2 ", len(tuple2)
        print "tuple3 ", len(tuple3)
        print "tuple4 ", len(tuple4)

        global merged;
        merged = [tuple0, tuple1, tuple2, tuple3, tuple4]
        print merged
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


def worker_join(i, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection, range, min1):
    """thread worker function"""
    cursor = openconnection.cursor()
    each_range = float(range)/5.0
    print 'each range ', each_range
    print 'Worker: %s' % i
    if i == 0:
        start = min1 + 0.0
        end = min1 + each_range
        print 'start ', start
        print 'end ', end
        cursor.execute("select * FROM %s INNER JOIN %s on %s.%s = %s.%s where %s.%s>=%s AND %s.%s<=%s order by %s.%s" %(InputTable1, InputTable2, InputTable1, Table1JoinColumn, InputTable2,  Table2JoinColumn, InputTable1, Table1JoinColumn, start, InputTable1, Table1JoinColumn, end, InputTable1, Table1JoinColumn))
        global tuple0;
        tuple0 = cursor.fetchall()

    elif i == 1:
        start = min1 + each_range
        end = min1 + (2*each_range)
        print 'start ', start
        print 'end ', end
        cursor.execute(
            "select * FROM %s INNER JOIN %s on %s.%s = %s.%s where %s.%s>%s AND %s.%s<=%s order by %s.%s" % (
            InputTable1, InputTable2, InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, InputTable1,
            Table1JoinColumn, start, InputTable1, Table1JoinColumn, end, InputTable1, Table1JoinColumn))
        global tuple1
        tuple1 = cursor.fetchall()

    elif i == 2:
        start = min1 + (2 * each_range)
        end = min1 + (3 * each_range)
        print 'start ', start
        print 'end ', end
        cursor.execute(
            "select * FROM %s INNER JOIN %s on %s.%s = %s.%s where %s.%s>%s AND %s.%s<=%s order by %s.%s" % (
            InputTable1, InputTable2, InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, InputTable1,
            Table1JoinColumn, start, InputTable1, Table1JoinColumn, end, InputTable1, Table1JoinColumn))
        global tuple2
        tuple2 = cursor.fetchall()

    elif i == 3:
        start = min1 + (3 * each_range)
        end = min1 + (4 * each_range)
        print 'start ', start
        print 'end ', end
        cursor.execute(
            "select * FROM %s INNER JOIN %s on %s.%s = %s.%s where %s.%s>%s AND %s.%s<=%s order by %s.%s" % (
                InputTable1, InputTable2, InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, InputTable1,
                Table1JoinColumn, start, InputTable1, Table1JoinColumn, end, InputTable1, Table1JoinColumn))
        global tuple3
        tuple3 = cursor.fetchall()

    elif i == 4:
        start = min1 + (4 * each_range)
        end = min1 + (5 * each_range)
        print 'start ', start
        print 'end ', end
        cursor.execute(
            "select * FROM %s INNER JOIN %s on %s.%s = %s.%s where %s.%s>%s AND %s.%s<=%s order by %s.%s" % (
                InputTable1, InputTable2, InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, InputTable1,
                Table1JoinColumn, start, InputTable1, Table1JoinColumn, end, InputTable1, Table1JoinColumn))
        global tuple4
        tuple4 = cursor.fetchall()

    return




def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    rangePartition_join(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection)
    merge_insert_join(openconnection, InputTable1, OutputTable)



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='ddsassignment3'):
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
    con.commit()
    con.close()


# Donot change this function
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
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" % (ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d` + ",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


if __name__ == '__main__':
    try:
        # Creating Database ddsassignment2
        print "Creating Database named as ddsassignment2"
        createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment2 database"
        con = getOpenConnection();

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE,
                     'parallelJoinOutputTable', con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
