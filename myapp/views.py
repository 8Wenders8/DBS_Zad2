from django.shortcuts import render
from django.http import HttpResponse
import psycopg2
import json
import environ


def health(request):
    environment = environ.Env()
    environ.Env.read_env()
    connection = psycopg2.connect(
        dbname=environment('dbname'),
        user=environment('dbuser'),
        password=environment('dbpass'),
        host=environment('dbhost'),
        port=environment('dbport')
    )
    cursor_1 = connection.cursor()
    cursor_2 = connection.cursor()
    query_1 = cursor_1.execute('SELECT VERSION();')
    query_1 = cursor_1.fetchall()
    query_2 = cursor_2.execute("SELECT pg_database_size('dota2')/1024/1024 as dota2_db_size;")
    query_2 = cursor_2.fetchall()
    result = json.dumps({
        "pgsql": {
            "version": query_1[0][0],
            "dota2_db_size": query_2[0][0]
        }
    }
    )
    return HttpResponse(result)

