from django.http import HttpResponse
import psycopg2
import json
import environ


def connect():
    environment = environ.Env()
    environ.Env.read_env()
    connection = psycopg2.connect(
        dbname=environment('dbname'),
        user=environment('dbuser'),
        password=environment('dbpass'),
        host=environment('dbhost'),
        port=environment('dbport')
    )
    return connection


def health(request):
    connection = connect()
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


def patches(request):
    connection = connect()

    sql_query = "SELECT p.patch_version, p.patch_start_date, p.patch_end_date, id as match_id, " \
                "ROUND(duration::numeric/60, 2) as match_duration " \
                "FROM matches as m " \
                "RIGHT JOIN (SELECT name as patch_version, extract(epoch FROM release_date)::integer as patch_start_date, " \
                "LEAD(extract(epoch from release_date)::integer) over (order by id) patch_end_date from patches) as p on m.start_time " \
                "BETWEEN p.patch_start_date and p.patch_end_date WHERE m.id is not null; "

    cursor = connection.cursor()
    query_1 = cursor.execute(sql_query)
    query = cursor.fetchall()

    result_data = {
        "patches": []
    }

    for i in range(len(query)):
        patch_arr = {
            "patch_version": str(query[i][0]),
            "patch_start_date": query[i][1],
            "patch_end_date": query[i][2],
            "matches": [{
                "match_id": query[i][3],
                "duration": str(query[i][4])
            }]
        }
        result_data["patches"].append(patch_arr)

    return HttpResponse(json.dumps(result_data), content_type='application/json')


def game_exp(request, player_id):
    connection = connect()

    sql_query = " SELECT p.id as id, COALESCE(p.nick, 'unknown') as player_nick, heroes.localized_name as hero_localized_name, " \
                " SUM(COALESCE(match_pd.xp_hero, 0) + COALESCE(match_pd.xp_creep, 0) + COALESCE(match_pd.xp_other, 0) + COALESCE(match_pd.xp_roshan, 0)) as experiences_gained, " \
                " MAX(match_pd.level) as level_gained, ROUND(matches.duration::numeric/60, 2) as match_duration_minutes, " \
                " CASE WHEN match_pd.player_slot in (0,1,2,3,4) and matches.radiant_win = true THEN 1 " \
                " WHEN match_pd.player_slot in (0,1,2,3,4) and matches.radiant_win = false THEN 0 " \
                " WHEN match_pd.player_slot in (128, 129, 130, 131, 132) and matches.radiant_win = false THEN 1 " \
                " WHEN match_pd.player_slot in (128,128,130,131,132) and matches.radiant_win = true THEN 0 " \
                " END winner, matches.id as match_id " \
                " FROM players p " \
                " JOIN matches_players_details as match_pd on p.id = match_pd.player_id " \
                " JOIN heroes on match_pd.hero_id = heroes.id " \
                " JOIN matches on match_pd.match_id = matches.id " \
                " WHERE p.id = " + str(player_id) + " group by p.id, heroes.localized_name, matches.duration, matches.radiant_win, matches.id, match_pd.player_slot " \
                " order by match_id; "

    cursor = connection.cursor()
    query_1 = cursor.execute(sql_query)
    query = cursor.fetchall()

    result_data = {
        "id": query[0][0],
        "player_nick": query[0][1],
        "matches": []
    }

    for i in range(len(query)):
        match_arr = {
            "match_id": query[i][7],
            "hero_localized_name": query[i][2],
            "match_duration_minutes": float(query[i][5]),
            "experiences_gained": query[i][3],
            "level_gained": query[i][4],
            "winner": bool(query[i][6])
        }
        result_data["matches"].append(match_arr)

    return HttpResponse(json.dumps(result_data), content_type='application/json')


def game_objectives(request, player_id):
    connection = connect()

    sql_query = " SELECT p.id as id, COALESCE(p.nick, 'unknown') as player_nick, heroes.localized_name as hero_localized_name, matches.id, " \
                " COALESCE(game_objectives.subtype, 'NO_ACTION') as subtype," \
                " CASE WHEN COUNT(game_objectives.subtype) = 0 THEN 1 " \
                " WHEN COUNT(game_objectives.subtype) != 0 THEN COUNT(game_objectives.subtype) " \
                " END AS count_obj " \
                " FROM players p " \
                " JOIN matches_players_details on p.id = matches_players_details.player_id " \
                " JOIN heroes on matches_players_details.hero_id = heroes.id " \
                " JOIN matches on matches_players_details.match_id = matches.id " \
                " LEFT JOIN game_objectives on game_objectives.match_player_detail_id_1 = matches_players_details.id " \
                " WHERE p.id = " + str(player_id) + " group by p.id, heroes.localized_name, game_objectives.subtype, matches.id, matches_players_details.match_id " \
                " order by match_id, count_obj DESC, subtype; "

    cursor = connection.cursor()
    query_1 = cursor.execute(sql_query)
    query = cursor.fetchall()

    result_data = {
        "id": query[0][0],
        "player_nick": query[0][1],
        "matches": []
    }

    prev_id = -1

    for i in range(len(query)):
        if prev_id != query[i][3]:
            match_arr = {
                "match_id": query[i][3],
                "hero_localized_name": query[i][2],
                "actions": [{
                    "hero_action": query[i][4],
                    "count": query[i][5]
                }]
            }
        if query[i - 1][3] != query[i][3]:
            result_data["matches"].append(match_arr)
        elif prev_id == query[i][3]:
            temp_arr = {
                "hero_action": query[i][4],
                "count": query[i][5]
            }
            match_arr["actions"].append(temp_arr)
        prev_id = query[i][3]

    return HttpResponse(json.dumps(result_data), content_type='application/json')


def abilities(request, player_id):
    connection = connect()

    sql_query = "SELECT p.id as id, COALESCE(p.nick, 'unknown') as player_nick,heroes.localized_name as hero_localized_name, abilities.name as name, " \
               " MAX(ability_upgrades.level) as upgrade_level, " \
               " COUNT(abilities.name) as count, matches_players_details.match_id " \
               " FROM players p " \
               " JOIN matches_players_details on p.id = matches_players_details.player_id " \
               " JOIN heroes on matches_players_details.hero_id = heroes.id " \
               " JOIN ability_upgrades on matches_players_details.id = ability_upgrades.match_player_detail_id " \
               " JOIN abilities on ability_upgrades.ability_id = abilities.id " \
               " WHERE p.id = " + str(player_id) + " group by p.id,heroes.localized_name,matches_players_details.match_id,abilities.name " \
               " order by match_id, name, upgrade_level; "

    cursor = connection.cursor()
    query_1 = cursor.execute(sql_query)
    query = cursor.fetchall()

    result_data = {"id": query[0][0],
                   "player_nick": query[0][1],
                   "matches": []}

    prev_id = -1

    for i in range(len(query)):
        if prev_id != query[i][6]:
            match_arr = {
                "match_id": query[i][6],
                "hero_localized_name": query[i][2],
                "abilities": [{
                    "ability_name": query[i][3],
                    "count": query[i][5],
                    "upgrade_level": query[i][4]
                }]
            }
        if query[i - 1][6] != query[i][6]:
            result_data["matches"].append(match_arr)
        elif prev_id == query[i][6]:
            temp_arr = {
                "ability_name": query[i][3],
                "count": query[i][5],
                "upgrade_level": query[i][4]}
            match_arr["abilities"].append(temp_arr)
        prev_id = query[i][6]

    return HttpResponse(json.dumps(result_data), content_type='application/json')
