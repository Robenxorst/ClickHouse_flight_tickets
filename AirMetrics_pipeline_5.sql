-- 5 этап. Смотрим данные о пасадочных талонах
SHOW CREATE TABLE clickhouse_data.boarding_passes;

-- локальная таблица для сырых данных с посадочными талонами
create table boarding_passes_raw_local on cluster 'sharded'
(
    `ticket_no` UInt64,
    `flight_id` Int32,
    `flight_no` LowCardinality(String),
    `boarding_no` Int32,
    `seat_no` LowCardinality(String)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/{database}/{table}', 
    '{replica}'
)
ORDER BY (flight_no, flight_id)

-- Создаем Distributed таблицу для выполнения распределенных SELECT запросов
CREATE TABLE boarding_passes_raw ON CLUSTER 'sharded' AS boarding_passes_raw_local
ENGINE = Distributed(
    'sharded', 
    'nik_polovnikov',         
    'boarding_passes_raw_local', 
    cityHash64(flight_no)
)


-- Создаем локальную широкую таблицу
create table boarding_passes_wide_local on cluster 'sharded'
(
	-- дополнительные данные из boarding_passes_raw_local
    boarding_no Int32,
    seat_no LowCardinality(String),
    
    -- данные из ticket_flights
	ticket_no UInt64,
    flight_id Int32,
    flight_no LowCardinality(String),
    fare_conditions LowCardinality(String),
    amount Decimal(10, 2),
    
    -- данные из ранее полученной flights
    scheduled_departure DateTime,
    scheduled_arrival DateTime,
    departure_airport LowCardinality(String),
    arrival_airport LowCardinality(String),
    aircraft_code LowCardinality(String),

    departure_date Date materialized toDate(scheduled_departure),
    distance_km Float64,
    ask Float64,
    
    -- дополнительно используем служебный столбец
    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree(
	-- указываем путь с макросом {shard} для корректного шардирования
    '/clickhouse/tables/{shard}/{database}/{table}', 
    '{replica}'
)
partition by toYYYYMM(departure_date)
ORDER BY (flight_no, departure_date, ticket_no)

-- создаем Distributed для выполнения SELECT запросов
CREATE TABLE boarding_passes_wide ON CLUSTER 'sharded' AS boarding_passes_wide_local
ENGINE = Distributed(
    'sharded', 
    'nik_polovnikov',         
    'boarding_passes_wide_local', 
    cityHash64(flight_no)
)

-- создание материализованного представления;
CREATE MATERIALIZED VIEW boarding_passes_raw_to_wide_mv on cluster 'sharded'
to boarding_passes_wide_local 
as 
select
	-- поля из триггерной таблицы
	board.ticket_no as ticket_no,
    board.flight_id as flight_id,
    board.flight_no as flight_no,
    board.boarding_no as boarding_no,
    board.seat_no as seat_no,
    
    -- поля с данными о билетах
    tf.fare_conditions as fare_conditions,
    tf.amount as amount,
    
    -- поля из дополнительной таблицы flights
    f.scheduled_departure AS scheduled_departure,
    f.scheduled_arrival AS scheduled_arrival,
    f.departure_airport AS departure_airport,
    f.arrival_airport AS arrival_airport,
    f.aircraft_code AS aircraft_code,
    
    -- Забираем готовые расчеты, которые сделало первое MV
    f.distance_km AS distance_km,
    f.ask AS ask,
    now() AS inserted_at

-- mv будет реагировать на изменения в первой таблице boarding_passes_raw_local
from boarding_passes_raw_local board 
ANY LEFT JOIN ticket_flights_raw_local tf on board.flight_id = tf.flight_id and board.ticket_no = tf.ticket_no 
ANY LEFT JOIN flights f ON tf.flight_id = f.flight_id;

-- заполняем таблицы данными из изначального источника
INSERT INTO boarding_passes_raw_local
select * from clickhouse_data.boarding_passes;



-- Все выполненные запросы по таблицам boarding_passes для контроля;
SELECT event_time, query 
FROM system.query_log
where user = 'clickhouse_student_nik_polovnikov'
and query like '%boarding_passes%'
and is_initial_query = true
and type = 'QueryFinish'
LIMIT 100;
