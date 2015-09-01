/*
    Params
    ------
    statsday = stats day (also day of messages, and active user file)

*/

SET pig.name 'Process aws flume message stream .... ';

SET default_parallel 10;

REGISTER s3://voxer-analytics/pig/external-udfs/Pigitos-1.0-SNAPSHOT.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/datafu-0.0.5.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/json-simple-1.1.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/guava-11.0.2.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/elephant-bird-core-3.0.0.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/elephant-bird-pig-3.0.0.jar;
REGISTER s3://voxer-analytics/pig/external-udfs/oink.jar;

/*
our own UDFs, written in python
*/
--REGISTER s3://voxer-analytics/pig/external-udfs/python/test.py USING jython AS testfunc;
REGISTER s3://voxer-analytics/pig/external-udfs/python/json_parse.py USING jython AS JsonParse;

DEFINE jsonStorage com.voxer.data.pig.store.JsonStorage();
DEFINE timestampToDay com.voxer.data.pig.eval.TimestampConverter('yyyyMMdd');
DEFINE timestampToDOW com.voxer.data.pig.eval.TimestampConverter('E');
DEFINE timestampToHOD com.voxer.data.pig.eval.TimestampConverter('H');
DEFINE domainFilter com.voxer.data.pig.eval.EmailDomainFilter();
DEFINE datestampToMillis com.voxer.data.pig.eval.DatestampConverter('yyyyMMdd','GMT');

DEFINE DistinctBy datafu.pig.bags.DistinctBy('0');
DEFINE SetDifference datafu.pig.sets.SetDifference();


IMPORT 's3://voxer-analytics/pig/macros/aws-load-profiles-raw.pig';
IMPORT 's3://voxer-analytics/pig/macros/aws-load-active-users.pig';

/*
modify TJ's AWS filter in order to quantify search
*/

aws_raw = LOAD 's3://voxer-flume/prod/{$statsday}00/aws*' USING jsonStorage as (json:map[]);

aws_contacts = FILTER aws_raw BY json#'event' == 'contacts';

/*
use UDF specific to parsing contact updates
*/

aws_contacts = FOREACH aws_contacts GENERATE JsonParse.parse_contact() as temp_tup;

aws_contacts = FOREACH aws_contacts GENERATE temp_tup.$0 AS user_id:chararray,
                                            temp_tup.$1 AS posted_time:double,
                                            temp_tup.$2 AS event:chararray,
                                            temp_tup.$3 AS recv_array;
                                            --temp_tup.$4 AS action:chararray;


aws_contacts = FOREACH aws_contacts GENERATE user_id, posted_time, event, FLATTEN(recv_array) AS (recv: chararray, action: chararray);

aws_contacts = FOREACH aws_contacts GENERATE user_id, posted_time, event, recv, action;

aws_search = FILTER aws_raw BY json#'event' == 'search';

--aws_search = FOREACH aws_search GENERATE json#'event' AS event:chararray,
--                                        json#'user_id' AS user_id:chararray,
--                                        (double)json#'posted_time' AS posted_time:double;

aws_search = FOREACH aws_search GENERATE json#'event' AS event:chararray,
                                        json#'user_id' AS user_id:chararray,
                                        (double)json#'posted_time' AS posted_time:double,
                                        json#'query'#'query'#'bool'#'must' AS query;

aws_search = FOREACH aws_search GENERATE event, user_id, posted_time, FLATTEN(query) AS query;

aws_search = FOREACH aws_search GENERATE user_id, posted_time, query#'query_string'#'query' AS query_string;

rmf s3://voxer-projects/ari/trash/search

STORE aws_search INTO 's3://voxer-projects/ari/trash/search' USING PigStorage();

/*
compute distribution of searches for a given searcher
*/

aws_search = FOREACH aws_search GENERATE user_id, query_string;

aws_search_group = GROUP aws_search BY user_id;

aws_search_dist = FOREACH aws_search_group GENERATE group AS user_id, COUNT(aws_search) AS attempts;

rmf s3://voxer-projects/ari/trash/search/dist

STORE aws_search_dist INTO 's3://voxer-projects/ari/trash/search/dist' USING PigStorage();

/*
Now, separate the matches
*/

aws_matches = FILTER aws_raw BY json#'event' == 'match_list';

/*
use custom UDF and match_list parsing function
*/

aws_matches = FOREACH aws_matches GENERATE JsonParse.parse_match() as temp_tup;

aws_matches = FOREACH aws_matches GENERATE temp_tup.$0 AS user_id:chararray,
                                            temp_tup.$1 AS posted_time:double,
                                            temp_tup.$2 AS event:chararray,
                                            temp_tup.$3 AS recv_array;


aws_matches = FOREACH aws_matches GENERATE user_id, posted_time, event, FLATTEN(recv_array) AS (recv: chararray);

aws_matches = FOREACH aws_matches GENERATE user_id, posted_time, event, recv;

--rmf s3://voxer-projects/temp_20150308/aws_search/matches

--STORE aws_matches INTO 's3://voxer-projects/temp_20150308/aws_search/matches' USING PigStorage();

/*
JOIN the search results with the contacts changes. First, find the unique set of users who searched
and join them with the contacts updates that where "add" events.
*/

temp = FOREACH aws_search GENERATE user_id;

search_unique = DISTINCT temp;

search_unique = FOREACH search_unique GENERATE user_id;

--rmf s3://voxer-projects/temp_20150308/aws_search/search_unique

--STORE search_unique INTO 's3://voxer-projects/temp_20150308/aws_search/search_unique' USING PigStorage();

contacts_filter = FILTER aws_contacts BY action == 'add';

contacts_add = FOREACH contacts_filter GENERATE user_id, posted_time, recv AS recv_id, action;

search_contact_join = JOIN search_unique BY user_id, contacts_add BY user_id;

search_contacts = FOREACH search_contact_join GENERATE search_unique::user_id AS user_id:chararray,
                                                        contacts_add::posted_time AS posted_time:double,
                                                        contacts_add::recv_id AS recv_id:chararray,
                                                        contacts_add::action AS add:chararray;

search_contacts_pair = FOREACH search_contacts GENERATE (user_id, recv_id) AS pair;

--rmf s3://voxer-projects/temp_20150308/aws_search/search-contacts_pair

--STORE search_contacts INTO 's3://voxer-projects/temp_20150308/aws_search/search-contacts_pair' USING PigStorage();

--rmf s3://voxer-projects/temp_20150308/aws_search/search-contacts

--STORE search_contacts INTO 's3://voxer-projects/temp_20150308/aws_search/search-contacts' USING PigStorage();

/*
Join matches with with contact updates (adds)
*/


contacts_add_pair = FOREACH contacts_add GENERATE (user_id, recv_id) AS pair;

matches_pair = FOREACH aws_matches GENERATE (user_id, recv) AS pair;

contact_matches_pair_join = JOIN contacts_add_pair BY pair, matches_pair BY pair;

contact_matches_pair = FOREACH contact_matches_pair_join GENERATE matches_pair::pair AS pair;

--rmf s3://voxer-projects/temp_20150308/aws_search/contact_matches_pair

--STORE contact_matches_pair INTO 's3://voxer-projects/temp_20150308/aws_search/contact_matches_pair' USING PigStorage();



contacts_add_matches_join = JOIN contacts_add BY user_id, aws_matches BY user_id;

contacts_add_matches = FOREACH contacts_add_matches_join GENERATE contacts_add::user_id AS user_id:chararray,
                                                                    contacts_add::posted_time AS posted_time:double,
                                                                    aws_matches::posted_time AS posted_time_match:double,
                                                                    contacts_add::recv_id AS recv_id:chararray,
                                                                    contacts_add::action AS action:chararray;


--rmf s3://voxer-projects/temp_20150308/aws_search/match-contacts

--STORE contacts_add_matches INTO 's3://voxer-projects/temp_20150308/aws_search/match-contacts' USING PigStorage();

/*
Now get the set difference between the two with a LEFT OUTER JOIN
*/

only_search_contacts_left = JOIN search_contacts_pair BY pair LEFT OUTER, contact_matches_pair BY pair;

only_search_contacts = FOREACH only_search_contacts_left GENERATE search_contacts_pair::pair AS pair,
                                                                (contact_matches_pair::pair IS NULL ? 'match' : 'search')  AS contact_pair;
--STORE only_search_contacts INTO 's3://voxer-projects/temp_20150308/aws_search/only_search_contacts' USING PigStorage();

/*
this shit here is counter-intuitive
*/
only_search_contacts = FILTER only_search_contacts BY contact_pair == 'match';

only_search_contacts = FOREACH only_search_contacts GENERATE pair;

only_search_contacts = DISTINCT only_search_contacts;

only_search_contacts = FOREACH only_search_contacts GENERATE pair.$0 AS searcher:chararray,
                                                             pair.$1 AS update:chararray;
--rmf s3://voxer-projects/temp_$statsday/aws_search/only_search_contacts

--STORE only_search_contacts INTO 's3://voxer-projects/temp_$statsday/aws_search/only_search_contacts' USING PigStorage();

/*
Count the number of contact additions per searcher
*/

temp_search  = FOREACH only_search_contacts GENERATE searcher AS user_id;

temp_search_group = GROUP temp_search BY user_id;

only_search_contacts_dist = FOREACH temp_search_group GENERATE group AS user_id, COUNT(temp_search) AS hits;

rmf s3://voxer-projects/temp_$statsday/aws_search/only_search_contacts_dist

STORE only_search_contacts_dist INTO 's3://voxer-projects/temp_$statsday/aws_search/only_search_contacts_dist' USING PigStorage();


/*
Now, 
*/

/*
Next steps: contruct some summary stats into some schema and STORE into a file
*/

/*
quantify search for new users on their day zero. (1) load in from TJ's daily-stats (2) parse, extract user_id, date, system
(3) JOIN with only_search_contacts
*/

A = LOAD '/temp_file' USING PigStorage(' ') AS (user_id: chararray, date: chararray, system: chararray);
B = FOREACH A GENERATE user_id, date, system;
C = FILTER B BY date == '$statsday';

cohorts = FOREACH C GENERATE user_id, date, system;

--rmf s3://voxer-projects/temp_$statsday/load_examples

--STORE cohorts INTO 's3://voxer-projects/temp_$statsday/load_examples' USING PigStorage();

--cat s3://voxer-projects/temp_$statsday/load_examples/part*

search_cohort_join = JOIN only_search_contacts BY searcher, cohorts BY user_id;

search_cohort = FOREACH search_cohort_join GENERATE cohorts::user_id AS user_id: chararray,
                                                    cohorts::system AS system: chararray,
                                                    only_search_contacts::update AS new_contact: chararray;

--rmf s3://voxer-projects/temp_$statsday/aws_search/search_cohort.$statsday
                                                    
--STORE search_cohort INTO 's3://voxer-projects/temp_$statsday/aws_search/search_cohort.$statsday' USING PigStorage(); 


/*
Let's make some counts:
S == number of Unique SEARCHERS
N = number of new users
A = number of daily active users
SC === number of contacts updates that resulted from search
SCN == number of SC that are due to new users
SCA = SA - SCN = number of contact updates resulting from search that are due to non-new users
R1 = SCN/SC
R2 = SCN/N
R3 = SC/S
R4 = SCA/A
*/


S_GROUP = GROUP search_unique ALL;
S_COUNT = FOREACH S_GROUP GENERATE COUNT(search_unique) AS S;

SC_GROUP = GROUP only_search_contacts ALL;
SC_COUNT = FOREACH SC_GROUP GENERATE COUNT(only_search_contacts) AS SC;

N_GROUP = GROUP cohorts ALL;
N_COUNT = FOREACH N_GROUP GENERATE COUNT(cohorts) AS N;

SCN_GROUP = GROUP search_cohort ALL;
SCN_COUNT = FOREACH SCN_GROUP GENERATE COUNT(search_cohort) AS SCN;

X = CROSS S_COUNT, SC_COUNT, N_COUNT, SCN_COUNT;

DUMP X;

rmf s3://voxer-projects/output/ari/$statsday/aws_search/summary_stats

STORE X INTO 's3://voxer-projects/output/ari/$statsday/aws_search/summary_stats' USING PigStorage();

/*
Scatter plot of searches vs. 
*/




 




