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


def abilities(request, player_id):
    environment = environ.Env()
    environ.Env.read_env()
    connection = psycopg2.connect(
        dbname=environment('dbname'),
        user=environment('dbuser'),
        password=environment('dbpass'),
        host=environment('dbhost'),
        port=environment('dbport')
    )
    sqlQuery = "SELECT p.id as id, COALESCE(p.nick, 'unknown') as player_nick, heroes.localized_name as hero_localized_name, matches.id, " \
               " COALESCE(game_objectives.subtype, 'NO ACTION') as subtype," \
               " CASE WHEN COUNT(game_objectives.subtype) = 0 THEN 1 " \
               " WHEN COUNT(game_objectives.subtype) != 0 THEN COUNT(game_objectives.subtype) " \
               " END AS count_obj " \
               " FROM players p " \
               " JOIN matches_players_details on p.id = matches_players_details.player_id " \
               " JOIN heroes on matches_players_details.hero_id = heroes.id " \
               " JOIN matches on matches_players_details.match_id = matches.id " \
               " LEFT JOIN game_objectives on game_objectives.match_player_detail_id_1 = matches_players_details.id " \
               " WHERE p.id = " + str(player_id) + "" \
               " group by p.id, heroes.localized_name, game_objectives.subtype, matches.id, matches_players_details.match_id " \
               " order by match_id, count_obj DESC, subtype; "

    cursor = connection.cursor()
    query_1 = cursor.execute(sqlQuery)
    #query = cursor.fetchall()
    query = cursor.fetchall()

    result = {"id": query[0][0],
            "player_nick": query[0][1],
            "matches": []}

    return HttpResponse(json.dumps(result), content_type='application/json')
