CREATE EXTERNAL TABLE IF NOT EXISTS basics_ext (
	tconst STRING,
	titleType STRING,
	primaryTitle STRING,
	originalTitle STRING,
	isAdult BOOLEAN,
	startYear INT,
	endYear VARCHAR(4),
	runtimeMinutes INT,
	genres ARRAY<STRING>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/${system:user.name}/projekt1/basics';



CREATE EXTERNAL TABLE IF NOT EXISTS mapreduce_output_ext (
	tconst STRING,
	quantity INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/${system:user.name}/projekt1/hadoop/mapreduce/output';




CREATE EXTERNAL TABLE if not exists result
(genre STRING,  num_actors INT)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION "/user/${system:user.name}/result";
ADD JAR /usr/lib/hive/lib/hive-hcatalog-core.jar;
INSERT INTO result
select single_genre, sum(quantity) as actors
from
(select tconst, single_genre 
	        from basics_ext lateral view explode(genres) adTable as single_genre
		        where titleType = 'movie') table1
		join mapreduce_output_ext table2 on table1.tconst = table2.tconst
		group by single_genre
		order by actors desc
		LIMIT 3;

