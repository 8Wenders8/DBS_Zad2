
## 'v2/patches/'
```sql
SELECT p.patch_version, p.patch_start_date, p.patch_end_date, id as match_id,
ROUND(duration::numeric/60, 2) as match_duration
FROM matches as m	
RIGHT JOIN (SELECT name as patch_version, extract(epoch FROM release_date)::integer as patch_start_date,
LEAD(extract(epoch from release_date)::integer) over (order by id) patch_end_date from patches) as p on m.start_time
BETWEEN p.patch_start_date and p.patch_end_date WHERE m.id is not null; 
```

## 'v2/players/{player_id}/game_exp/'
```sql
SELECT p.id as id, COALESCE(p.nick, 'unknown') as player_nick, heroes.localized_name as hero_localized_name,
SUM(COALESCE(match_pd.xp_hero, 0) + COALESCE(match_pd.xp_creep, 0) + COALESCE(match_pd.xp_other, 0) + COALESCE(match_pd.xp_roshan, 0)) as experiences_gained,
MAX(match_pd.level) as level_gained, ROUND(matches.duration::numeric/60, 2) as match_duration_minutes, 
CASE WHEN match_pd.player_slot in (0,1,2,3,4) and matches.radiant_win = true THEN 1 
WHEN match_pd.player_slot in (0,1,2,3,4) and matches.radiant_win = false THEN 0 
WHEN match_pd.player_slot in (128, 129, 130, 131, 132) and matches.radiant_win = false THEN 1
WHEN match_pd.player_slot in (128,128,130,131,132) and matches.radiant_win = true THEN 0
END winner, matches.id as match_id
FROM players p
JOIN matches_players_details as match_pd on p.id = match_pd.player_id
JOIN heroes on match_pd.hero_id = heroes.id 
JOIN matches on match_pd.match_id = matches.id 
WHERE p.id = {player_id}  group by p.id, heroes.localized_name, matches.duration, matches.radiant_win, matches.id, match_pd.player_slot
order by match_id;
```

## 'v2/players/{player_id}/game_objectives/'
```sql
SELECT p.id as id, COALESCE(p.nick, 'unknown') as player_nick, heroes.localized_name as hero_localized_name, matches.id, 
COALESCE(game_objectives.subtype, 'NO_ACTION') as subtype,
CASE WHEN COUNT(game_objectives.subtype) = 0 THEN 1 
WHEN COUNT(game_objectives.subtype) != 0 THEN COUNT(game_objectives.subtype) 
END AS count_obj
FROM players p 
JOIN matches_players_details on p.id = matches_players_details.player_id 
JOIN heroes on matches_players_details.hero_id = heroes.id 
JOIN matches on matches_players_details.match_id = matches.id
LEFT JOIN game_objectives on game_objectives.match_player_detail_id_1 = matches_players_details.id
WHERE p.id = {player_id}  group by p.id, heroes.localized_name, game_objectives.subtype, matches.id, matches_players_details.match_id
order by match_id, count_obj DESC, subtype;
```

## 'v2/players/{player_id}/abilities/'
```sql
SELECT p.id as id, COALESCE(p.nick, 'unknown') as player_nick,heroes.localized_name as hero_localized_name, abilities.name as name,
MAX(ability_upgrades.level) as upgrade_level,
COUNT(abilities.name) as count, matches_players_details.match_id
FROM players p
JOIN matches_players_details on p.id = matches_players_details.player_id
JOIN heroes on matches_players_details.hero_id = heroes.id 
JOIN ability_upgrades on matches_players_details.id = ability_upgrades.match_player_detail_id
JOIN abilities on ability_upgrades.ability_id = abilities.id
WHERE p.id = {player_id} group by p.id,heroes.localized_name,matches_players_details.match_id,abilities.name
order by match_id, name, upgrade_level; 
```

## 'v3/matches/{match_id}/top_purchases/'
```sql
WITH bi AS (SELECT h.localized_name AS hero_name, i.name AS item_name, hero_id, item_id, match_id FROM purchase_logs AS pl
NNER JOIN items AS i ON item_id = i.id 
NNER JOIN matches_players_details AS mpd ON match_player_detail_id = mpd.id
NNER JOIN heroes AS h ON mpd.hero_id = h.id
NNER JOIN matches AS m ON mpd.match_id = m.id
HERE m.id = {match_id} AND ((mpd.player_slot BETWEEN 0 AND 4 AND m.radiant_win = TRUE) OR (mpd.player_slot BETWEEN 128 AND 132 AND m.radiant_win = FALSE))),
ci AS (SELECT DISTINCT bi.hero_id, bi.item_id, COUNT(bi.item_id) AS num, bi.match_id, bi.hero_name, bi.item_name FROM bi GROUP BY bi.item_id, bi.hero_id, bi.match_id, bi.hero_name, bi.item_name),
top AS (SELECT ci.match_id, ci.hero_name, ci.item_name, row_number() over (PARTITION BY ci.hero_id ORDER BY ci.num DESC, ci.item_name ) row_num, ci.hero_id, ci.item_id, ci.num FROM ci ORDER BY ci.hero_id)
SELECT top.match_id, top.hero_id, top.hero_name, top.item_id, top.item_name, top.num FROM top WHERE row_num BETWEEN 1 AND 5;
```
