__author__ = 'debalid'

# Enthusiasts, 2015

from psycopg2 import pool


class PostgresInjection(object):
    __dbConfig = dict(dbhost="localhost", dbport=5432,
                      dbname="roxana", dbuser="roxana_consumer", dbpassword="qwerty1234")

    def one(self):
        return PostgresConnectionOwner(self.__pool)

    def __init__(self):
        print("INJECTED")
        self.__pool = pool.ThreadedConnectionPool(1, 20,
                                          host=self.__dbConfig["dbhost"],
                                          port=self.__dbConfig["dbport"],
                                          database=self.__dbConfig["dbname"],
                                          user=self.__dbConfig["dbuser"],
                                          password=self.__dbConfig["dbpassword"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__pool.closeall()


class PostgresConnectionOwner(object):
    def __init__(self, conn_pool):
        import uuid
        self.__pool = conn_pool
        self.__key = uuid.uuid1().hex

    def __enter__(self):
        self.connection = self.__pool.getconn(key=self.__key)
        self.connection.set_client_encoding("UTF-8")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print("Rollback")
            print(exc_val)
            self.connection.rollback()
        else:
            self.connection.commit()
        self.__pool.putconn(self.connection, key=self.__key)
