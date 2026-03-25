-- 1 этап. Создаем словари для Airports и Aircrafts;

SHOW CREATE TABLE clickhouse_data.airports;
SHOW CREATE TABLE clickhouse_data.aircrafts;

CREATE DICTIONARY aircrafts_dict ON CLUSTER 'all-replicated'
(
    `aircraft_code` String,
    `model` String,
    `range` Int32,
    `capacity` UInt16
)
PRIMARY KEY aircraft_code
SOURCE(CLICKHOUSE(
    DB 'clickhouse_data'
    TABLE 'aircrafts'
    USER '{user_name}'
    PASSWORD '{password}'
))
LAYOUT(HASHED())
LIFETIME(MIN 3600 MAX 7200); -- задаем период обновления между 1 и 2 часами;

-- Аналогичная операция c другой таблицей
-- Обязательно добавляем права на доступ чтения,
-- так как будем обновлять словари из clickhouse_data;
CREATE DICTIONARY airports_dict ON CLUSTER 'all-replicated'
(
    `airport_code` String,
    `airport_name` String,
    `city` String,
    `longitude` Float32,
    `latitude` Float32,
    `timezone` String
)
PRIMARY KEY airport_code
SOURCE(CLICKHOUSE(
    DB 'clickhouse_data'
    TABLE 'airports'
    USER '{user_name}'
    PASSWORD '{password}'
))
LAYOUT(HASHED())
LIFETIME(MIN 3600 MAX 7200);

select * from airports_dict limit 10;


-- Этап 2. Создание материализированных представлений для flights и flight_statuses
SHOW CREATE TABLE clickhouse_data.flights;
show create table clickhouse_data.flight_statuses;

-- Для простоты считаем, что если Null в столбце нет, то он и не нужен;
SELECT COUNT(distinct departure_airport) FROM clickhouse_data.flights 
WHERE aircraft_code is NOT NULL ;

-- Для строковых столбцов с небольшим числом значений введем LowCardinality
-- Зададим таблицу как ReplicatedMergeTree
create table flights_raw ON CLUSTER 'all-replicated'
(
	`flight_id` UInt32,
    `flight_no` String,
    `scheduled_departure` DateTime,
    `scheduled_arrival` DateTime,
    `departure_airport` LowCardinality(String),
    `arrival_airport` LowCardinality(String),
    `aircraft_code` LowCardinality(String)
)
ENGINE = ReplicatedMergeTree(
	-- Путь в ZooKeeper
    '/clickhouse/tables/global/{database}/{table}',
    '{replica}'                                  
)
ORDER BY flight_id;

-- дропаем плохо созданную таблицу
-- DROP TABLE flights_raw ON CLUSTER 'all-replicated';

create table flights ON CLUSTER 'all-replicated'
(
	flight_id UInt32,
    flight_no String,
    scheduled_departure DateTime,
    scheduled_arrival DateTime,
    departure_airport LowCardinality(String),
    arrival_airport LowCardinality(String),
    aircraft_code LowCardinality(String),
    
    -- Материализованный столбец, рассчитывается при insert
    -- В Select следует указывать его явно
    departure_date Date materialized toDate(scheduled_departure),
    -- дистанция между двумя аэропортами
    distance_km Float64,
    -- ask - это метрика вместимости на расстояние;
    ask Float64,
	
    -- дополнительно используем служебный столбец
    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/global/{database}/{table}',
    '{replica}'
)
ORDER BY (flight_no, departure_date);

-- создадим материализованное представление для
-- быстрого заполнения данных из flights_raw во flights.
-- Заметка: все нужное уже есть в словарях.

CREATE MATERIALIZED VIEW flights_mv on cluster 'all-replicated'
to flights as 
select
	flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    aircraft_code,
    geoDistance(
        dictGet('airports_dict', 'longitude', departure_airport),
        dictGet('airports_dict', 'latitude', departure_airport),
        dictGet('airports_dict', 'longitude', arrival_airport),
        dictGet('airports_dict', 'latitude', arrival_airport)
    ) / 1000 AS distance_km,
    dictGet('aircrafts_dict', 'capacity', aircraft_code) * distance_km AS ask,
    now() AS inserted_at
from flights_raw;

-- дропнули созданную БД
--DROP VIEW flights_mv ON CLUSTER 'all-replicated';

-- вставляем несколько строк в flights_raw
INSERT INTO flights_raw
SELECT * FROM clickhouse_data.flights
LIMIT 20;

-- посмортим сработало ли mv
select * from flights;
-- Имеем 20 строчек. Расстояния считаются корректно


-- Создадим таблицу аналогичным образом для flight_statuses
SHOW CREATE TABLE clickhouse_data.flight_statuses;

create table flight_statuses ON CLUSTER 'all-replicated'
(
    flight_id UInt32,
    flight_no String,
    scheduled_departure DateTime,
    status String,
    actual_departure Nullable(DateTime),
    actual_arrival Nullable(DateTime),
    departure_date Date materialized toDate(scheduled_departure),
    -- служебный столбец для определения актуальности записи
    updated_at DateTime DEFAULT now()
)
-- используем реплицированый движок убирающий дубли
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/global/{database}/{table}',
    '{replica}',
    updated_at
)
ORDER BY (flight_no, departure_date);

-- добавляем и проверяем данные
INSERT INTO flight_statuses
SELECT *, now() as updated_at FROM clickhouse_data.flight_statuses
LIMIT 20;

select * from flight_statuses FINAL;

-- подчищаем данные
TRUNCATE TABLE flights ON CLUSTER 'all-replicated';

-- заливаем свежие данные
INSERT INTO flight_statuses
SELECT *, now() as updated_at FROM clickhouse_data.flight_statuses;

select * from flights_raw;

INSERT INTO flights_raw
SELECT * FROM clickhouse_data.flights;

-- подтянулось ли через VIEW ?
select COUNT() from flights;


