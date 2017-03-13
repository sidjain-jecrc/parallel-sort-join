#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
from threading import Thread
import heapq

FIRST_TABLE_NAME = 'employee'
SECOND_TABLE_NAME = 'department'
SORT_COLUMN_NAME_FIRST_TABLE = 'esal'
SORT_COLUMN_NAME_SECOND_TABLE = 'dsal'
JOIN_COLUMN_NAME_FIRST_TABLE = 'deptid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'deptid'
##########################################################################################################
threadList0 = []
threadList1 = []
threadList2 = []
threadList3 = []
threadList4 = []


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    cur = openconnection.cursor()
    cur.execute('SELECT ' + SortingColumnName + ' from ' + InputTable + ';')

    # Logic to divide the table data in round robin manner for 5 threads
    if bool(cur.rowcount):
        rows = cur.fetchall()
        lastInserted = 0

        for row in rows:
            lastInserted = lastInserted % 5

            if lastInserted == 0:
                threadList0.append(row[0])
            elif lastInserted == 1:
                threadList1.append(row[0])
            elif lastInserted == 2:
                threadList2.append(row[0])
            elif lastInserted == 3:
                threadList3.append(row[0])
            elif lastInserted == 4:
                threadList4.append(row[0])

            lastInserted += 1

    # Allocate each thread separate list
    for i in range(5):
        if (i == 0):
            a = threadList0
        elif (i == 1):
            a = threadList1
        elif (i == 2):
            a = threadList2
        elif (i == 3):
            a = threadList3
        elif (i == 4):
            a = threadList4

        t = Thread(target=columnSort, args=(a,))
        t.start()

    finalList = list(heapq.merge(threadList0, threadList1, threadList2, threadList3, threadList4))
    finalList = list(set(finalList))

    createOutputTable = "CREATE TABLE IF NOT EXISTS " + OutputTable + " AS Select * from " + InputTable + " where 1=2;"
    cur.execute(createOutputTable)
    openconnection.commit()

    # Logic to select rows from input table and writing them to output table
    for val in finalList:
        insertQuery = "INSERT INTO " + OutputTable + " SELECT * from " + InputTable + " where " + SortingColumnName + " = " + str(val) + ";"
        print insertQuery
        cur.execute(insertQuery)
        openconnection.commit()


def columnSort(threadList):
    threadList.sort()


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()

    if Table1JoinColumn != Table2JoinColumn:
        create_output_table_query = "CREATE TABLE IF NOT EXISTS " + OutputTable + " AS SELECT * FROM " + InputTable1 + " JOIN " + InputTable2 + " on " + Table1JoinColumn + "=" + Table2JoinColumn + " where 1=2;"
        cur.execute(create_output_table_query)
    else:
        rename_column_name = str(Table1JoinColumn) + "_table1"
        alter_query = "ALTER TABLE " + InputTable1 + " RENAME COLUMN " + Table1JoinColumn + " TO " + rename_column_name
        Table1JoinColumn = rename_column_name
        cur.execute(alter_query)
        create_output_table_query = "CREATE TABLE IF NOT EXISTS " + OutputTable + " AS SELECT * FROM " + InputTable1 + " JOIN " + InputTable2 + " on " + Table1JoinColumn + "=" + Table2JoinColumn + " where 1=2;"
        cur.execute(create_output_table_query)

    openconnection.commit()

    firstList = []
    selectQueryOne = "SELECT " + Table1JoinColumn + " from " + InputTable1
    cur.execute(selectQueryOne)
    rows = cur.fetchall()
    for row in rows:
        firstList.append(row[0])

    max_val = max(firstList)
    min_val = min(firstList)

    step_size = max_val / 5
    min_step_range = min_val - 5
    max_step_range = step_size + 5

    # Allocate each thread separate list
    for i in range(5):
        min_range_value = min_step_range
        max_range_value = max_step_range
        t = Thread(target=tableJoin, args=(min_range_value, max_range_value, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection,))
        t.start()
        min_step_range = max_range_value
        max_step_range += step_size


def tableJoin(min_value, max_value, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()
    insert_join_query = "INSERT INTO " + OutputTable + "(SELECT * FROM " + InputTable1 + " JOIN " + InputTable2 + " on " + Table1JoinColumn + "=" + Table2JoinColumn + " where " + Table1JoinColumn + " > " + str(
        min_value) + " and " + Table1JoinColumn + " <= " + str(max_value) + ")"
    cur.execute(insert_join_query)
    openconnection.commit()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Do not change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Do not change this function
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


# Do not change this function
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


# Do not change this function
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
        print "Creating Database named as ddsassignment3"
        createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment3 database"
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
