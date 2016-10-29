#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import math

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    print 'Finding the number of partitions'
    try:
        cursor = openconnection.cursor()
        cursor.execute("select count(*) from information_schema.tables where table_name LIKE 'rangeratingspart%'")
        rows = cursor.fetchall()
        cursor.execute("DROP TABLE IF EXISTS numberOfPartitions");
        cursor.execute("CREATE TABLE IF NOT EXISTS numberOfPartitions(partition INT)");

        numberOfPartitions = 0
        for row in rows:
            numberOfPartitions = row[0]
            cursor.execute( "INSERT INTO numberOfPartitions(partition) values(%d)" %(row[0]) )

        low = int(math.ceil((ratingMinValue / 5) * numberOfPartitions))
        high = int(math.ceil((ratingMaxValue / 5) * numberOfPartitions))

        if low > 0:
            low = low - 1

        if high > 0:
            high = high - 1
        #print ' Low' , low , ' High ' , high;
        var = ''
        while low <= high:
            cursor.execute("SELECT * from rangeratingspart%s where rating>=%f and rating<=%f ORDER BY rating ASC" %(low,ratingMinValue,ratingMaxValue))
            rows = cursor.fetchall()
            for row in rows:
                var = var + 'rangeratingspart' + str(low)+ ',' + str(row[0]) + ',' + str(row[1])+ ',' + str(row[2]) + '\n'
            low += 1


        i = 0
        while i<numberOfPartitions:
            cursor.execute("SELECT * from RoundRobinRatingsPart%s where rating>=%f and rating<=%f ORDER BY rating ASC" % (i, ratingMinValue, ratingMaxValue))
            rows = cursor.fetchall()
            for row in rows:
                var = var + 'roundrobinratingspart' + str(i) + ',' + str(row[0]) + ',' + str(row[1]) + ',' + str(row[2]) + '\n'
            i += 1
        print var

        target = open("RangeQueryOut.txt", 'w')
        target.write(var)
        target.close()

        openconnection.commit();
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

    pass #Remove this once you are done with implementation

def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.
    try:
        cursor = openconnection.cursor()
        var = ''
        cursor.execute("select count(*) from information_schema.tables where table_name LIKE 'rangeratingspart%'")
        rows = cursor.fetchall()
        numberOfPartitions = 0
        for row in rows:
            numberOfPartitions = row[0]
        partition = int(math.ceil((ratingValue / 5) * numberOfPartitions))
        if partition > 0:
            partition = partition - 1
        cursor.execute("SELECT * from rangeratingspart%s where rating=%f ORDER BY rating ASC" % (partition, ratingValue))
        rows = cursor.fetchall()
        for row in rows:
            var = var + 'rangeratingspart' + str(partition) + ',' + str(row[0]) + ',' + str(row[1]) + ',' + str(row[2]) + '\n'
        i = 0
        while i < numberOfPartitions:
            cursor.execute("SELECT * from RoundRobinRatingsPart%s where rating=%f ORDER BY rating ASC" % (i, ratingValue))
            rows = cursor.fetchall()
            for row in rows:
                var = var + 'roundrobinratingspart' + str(i) + ',' + str(row[0]) + ',' + str(row[1]) + ',' + str(row[2]) + '\n'
            i += 1
        print var
        openconnection.commit();

        target = open("PointQueryOut.txt", 'w')
        target.write(var)
        target.close()

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

    pass # Remove this once you are done with implementation
