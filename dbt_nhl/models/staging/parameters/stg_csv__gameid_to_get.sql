with
agenda AS (
	SELECT
	*
	FROM {{ source('nhl_raw','raw_game_info') }}
	WHERE
	"gamestateid" IN ('1','7')
	AND "gameschedulestateid" = '1'
	AND "gametype" IN ('2','3')
	AND "period" IS NOT NULL
	),
got_data AS (
	SELECT * FROM {{ source('nhl_raw','raw_boxscore_games') }}
	)
SELECT a.* FROM agenda a
LEFT JOIN got_data gd
ON a.id = gd.id
WHERE gd.id IS NULL
AND to_date(a."gamedate",'YYYY-MM-DD') < current_date
ORDER BY id desc
