%sql 

create table if not exists default.airports
using DELTA
LOCATION '/FileStore/tables/airports/';

create table if not exists default.airlines
using DELTA
LOCATION '/FileStore/tables/airlines/';

create table if not exists default.flights
using DELTA
LOCATION '/FileStore/tables/partition';

---
%sql

CREATE external TABLE if not exists default.l1_flights (
  YEAR STRING,
  MONTH STRING,
  DAY STRING,
  DAY_OF_WEEK STRING,
  AIRLINE STRING,
  FLIGHT_NUMBER STRING,
  TAIL_NUMBER STRING,
  ORIGIN_AIRPORT STRING,
  DESTINATION_AIRPORT STRING,
  SCHEDULED_DEPARTURE STRING,
  DEPARTURE_TIME STRING,
  DEPARTURE_DELAY STRING,
  TAXI_OUT STRING,
  WHEELS_OFF STRING,
  SCHEDULED_TIME STRING,
  ELAPSED_TIME STRING,
  AIR_TIME STRING,
  DISTANCE STRING,
  WHEELS_ON STRING,
  TAXI_IN STRING,
  SCHEDULED_ARRIVAL STRING,
  ARRIVAL_TIME STRING,	
  ARRIVAL_DELAY STRING,
  DIVERTED STRING,
  CANCELLED STRING,
  CANCELLATION_REASON STRING,
  AIR_SYSTEM_DELAY STRING,
  SECURITY_DELAY STRING,
  AIRLINE_DELAY STRING,
  LATE_AIRCRAFT_DELAY STRING,
  WEATHER_DELAY STRING)
  LOCATION '/FileStore/tables/flights/';
  
-------------------------------------------------------------------------------------------------
-- Create level one data by eliminating duolicate.
-- Since there is no key column exist,using group by columns to eliminate duplicats
-------------------------------------------------------------------------------------------------

insert into  default.l1_flights
select 
  YEAR ,
  MONTH ,
  DAY ,
  DAY_OF_WEEK ,
  AIRLINE ,
  FLIGHT_NUMBER ,
  TAIL_NUMBER ,
  ORIGIN_AIRPORT ,
  DESTINATION_AIRPORT ,
  SCHEDULED_DEPARTURE ,
  DEPARTURE_TIME ,
  DEPARTURE_DELAY ,
  TAXI_OUT ,
  WHEELS_OFF ,
  SCHEDULED_TIME ,
  ELAPSED_TIME ,
  AIR_TIME ,
  DISTANCE ,
  WHEELS_ON ,
  TAXI_IN ,
  SCHEDULED_ARRIVAL ,
  ARRIVAL_TIME ,	
  ARRIVAL_DELAY ,
  DIVERTED ,
  CANCELLED ,
  CANCELLATION_REASON ,
  AIR_SYSTEM_DELAY ,
  SECURITY_DELAY ,
  AIRLINE_DELAY ,
  LATE_AIRCRAFT_DELAY ,
  WEATHER_DELAY 
  from  default.flights
  group by
  YEAR ,
  MONTH ,
  DAY ,
  DAY_OF_WEEK ,
  AIRLINE ,
  FLIGHT_NUMBER ,
  TAIL_NUMBER ,
  ORIGIN_AIRPORT ,
  DESTINATION_AIRPORT ,
  SCHEDULED_DEPARTURE ,
  DEPARTURE_TIME ,
  DEPARTURE_DELAY ,
  TAXI_OUT ,
  WHEELS_OFF ,
  SCHEDULED_TIME ,
  ELAPSED_TIME ,
  AIR_TIME ,
  DISTANCE ,
  WHEELS_ON ,
  TAXI_IN ,
  SCHEDULED_ARRIVAL ,
  ARRIVAL_TIME ,	
  ARRIVAL_DELAY ,
  DIVERTED ,
  CANCELLED ,
  CANCELLATION_REASON ,
  AIR_SYSTEM_DELAY ,
  SECURITY_DELAY ,
  AIRLINE_DELAY ,
  LATE_AIRCRAFT_DELAY ,
  WEATHER_DELAY 

-------------------------------------------------------------------------------------------------
-- Total number of flights by airline  and airport on a monthly basis.
-------------------------------------------------------------------------------------------------

WITH flight_count_origin
     AS (SELECT f.airline,
                f.origin_airport AS airport,
                f.month,
                Count(*)         AS counts
         FROM   default.l1_flights f
                JOIN default.airlines al
                  ON al.iata_code = f.airline
         WHERE  f.cancelled = 0
         GROUP  BY f.airline,
                   f.origin_airport,
                   f.month),
                   
     flight_count_destination
     AS (SELECT f.airline,
                f.destination_airport AS airport,
                f.month,
                Count(*)              AS counts
         FROM   default.l1_flights f
                JOIN default.airlines al
                  ON al.iata_code = f.airline
         WHERE  f.cancelled = 0
         GROUP  BY f.airline,
                   f.destination_airport,
                   f.month)
SELECT airline,
       airport,
       month,
       Sum(counts)
FROM   (SELECT airline,
               airport,
               month,
               counts
        FROM   flight_count_origin
        UNION
        SELECT airline,
               airport,
               month,
               counts
        FROM   flight_count_destination) AS temp
GROUP  BY airline,
          airport,
          month
ORDER  BY airline,
          month 

-------------------------------------------------------------------------------------------------
-- Airline with the most unique routes
-------------------------------------------------------------------------------------------------
WITH routes
     AS (SELECT airline,
                origin_airport,
                destination_airport,
                Count(*) AS cnt
         FROM   default.l1_flights
         GROUP  BY airline,
                   origin_airport,
                   destination_airport
         ORDER  BY airline,
                   cnt)
SELECT airline,
       origin_airport,
       destination_airport,
       Max(cnt) AS cnt
FROM   routes
WHERE  cnt = (SELECT Max(cnt)
              FROM   routes)
GROUP  BY airline,
          origin_airport,
          destination_airport 

-------------------------------------------------------------------------------------------------
--Airlines with the largest number of delays 
-------------------------------------------------------------------------------------------------
SELECT airline,
       Count(*) AS delay_cnt
FROM   default.l1_flights
WHERE  departure_delay > 0


-------------------------------------------------------------------------------------------------
-- On time percentage of each airline for the year 2015
-- Airlines with the largest number of delays 
-------------------------------------------------------------------------------------------------
WITH flight_time
     AS (SELECT airline,
                departure_delay,
                arrival_delay,
                CASE
                  WHEN departure_delay > 0
                        OR arrival_delay > 0 THEN "1"
                  ELSE "0"
                END AS delay
         FROM   default.l1_flights
         WHERE  year = '2015'),
     intime
     AS (SELECT airline,
                Count(*) intime_cnt,
                'intime'
         FROM   flight_time
         WHERE  delay = 1
         GROUP  BY airline),
     delay
     AS (SELECT airline,
                Count(*) delay_cnt,
                'delay'
         FROM   flight_time
         WHERE  delay = 0
         GROUP  BY airline),
     total
     AS (SELECT airline,
                Count(*) total_cnt,
                'total'
         FROM   flight_time
         GROUP  BY airline)
SELECT a.airline,
       intime_cnt,
       delay_cnt,
       total_cnt,
       Ceiling(( intime_cnt / total_cnt ) * 100) OnTimePercentage
FROM   intime a,
       delay d,
       total t
WHERE  a.airline = d.airline
       AND a.airline = t.airline
        OR arrival_delay > 0
GROUP  BY airline
ORDER  BY delay_cnt DESC 

