-- Задача номер 3-4. 
-- Используем кластер sharded, так как предполагаем, что данные огромные.

-- данные о билетах
SHOW CREATE TABLE clickhouse_data.ticket_flights;
-- посадочные талоны(пассажиры, которые сели в самолет)

select * from clickhouse_data.ticket_flights limit 10;

-- локальная таблица для сырых данных
create table ticket_flights_raw_local on cluster 'sharded'
(
    `ticket_no` UInt64,
    `flight_id` Int32,
    `flight_no` LowCardinality(String),
    `fare_conditions` LowCardinality(String),
    `amount` Decimal(10, 2)
)
ENGINE = ReplicatedMergeTree(
	-- указываем путь с макросом {shard} для корректного шардирования
    '/clickhouse/tables/{shard}/{database}/{table}', 
    '{replica}'
)
ORDER BY (flight_no, flight_id);

-- поверх локальной таблицы создаем Distributed
-- таблицу(по сути еще один движок);
CREATE TABLE ticket_flights_raw ON CLUSTER 'sharded' AS ticket_flights_raw_local
ENGINE = Distributed(
    'sharded', 
    'nik_polovnikov',         
    'ticket_flights_raw_local', 
    cityHash64(flight_no)
);


-- создаем широкую реплицированную таблицу(ticket_flights + flights)
create table ticket_flights_wide_local on cluster 'sharded'
(
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
ORDER BY (flight_no, departure_date, ticket_no);
-- Успешное создание таблицы!!!

-- Создаем Distributed таблицу для управления шардами кластера;
CREATE TABLE ticket_flights_wide ON CLUSTER 'sharded' AS ticket_flights_wide_local
ENGINE = Distributed(
    'sharded', 
    'nik_polovnikov',         
    'ticket_flights_wide_local', 
    cityHash64(flight_no)
);
-- Готово!!

-- Удаляем старую таблицу
drop table ticket_flights_wide ON CLUSTER 'sharded';


-- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! --
-- Создаем материализованное представление 

CREATE MATERIALIZED VIEW ticket_flights_raw_to_wide_mv on cluster 'sharded'
to ticket_flights_wide_local 
as 
select
	-- поля из триггерной таблицы
	tr.ticket_no as ticket_no,
    tr.flight_id as flight_id,
    tr.flight_no as flight_no,
    tr.fare_conditions as fare_conditions,
    tr.amount as amount,
    
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

-- mv будет реагировать на изменения в первой таблице, 
-- указанной в from
from ticket_flights_raw_local tr
-- лучше всего использовать 
-- ANY LEFT JOIN для устранения дубликатов
ANY LEFT JOIN flights f
ON tr.flight_id = f.flight_id;
-- И это правильный ответ!!! Мы получили максимум баллов

-- удаляем нашу таблицу:
-- drop table ticket_flights_raw_to_wide_mv on cluster 'sharded';

-- Вставим много строчек в таблицу с сырыми данными ticket_flights_raw 
-- (через Distributed() , так как имеем дело с шардированным кластером)
INSERT INTO ticket_flights_raw
select * from clickhouse_data.ticket_flights;

-- Проверим сработало ли наше материальное представление

-- обращение к Distributed
select * from ticket_flights_raw;

-- Обращение к локальной таблице хоста
select * from ticket_flights_raw_local;
-- похоже, что шардирование не работает! Все данные сохраняются на одном хосте, а должны делиться пополам
-- (мы и используем кластер sharded)

-- материальное представление при этом корректно заполняет широкую таблицу
select * from ticket_flights_wide_local;

-- удаляем все старые данные(затем обновляем)
TRUNCATE TABLE ticket_flights_wide_local 
ON CLUSTER 'sharded';

TRUNCATE TABLE ticket_flights_raw_local 
ON CLUSTER 'sharded';

