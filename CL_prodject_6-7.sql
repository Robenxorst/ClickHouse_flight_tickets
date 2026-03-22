-- 6-7. Создание агрегирующих таблиц.

show create table flights;
-- будем вставлять данные напрямую из локальных таблиц
show create table ticket_flights_wide_local;
show create table boarding_passes_wide_local;

-- Создаем шардированную таблицу SummingMergeTree
-- Заранее сделаем внутри нее проекцию!
create table flight_metrics_local on cluster 'sharded'
(
	-- дополнительное спорное поле, 
	-- которое нам не пригодилось для работы с 
    -- широкими таблицами.
	--	flight_id UInt32,
	
	departure_date Date,
	departure_airport LowCardinality(String),
	arrival_airport LowCardinality(String),
	aircraft_code LowCardinality(String),
	flight_no LowCardinality(String),
	
	-- суммирование у нас идет по ВСЕМ полям,
	-- которые мы не использовали в ключах сортировки
	
	-- метрика, что к нам приходит из таблицы 
	-- flights (остальные метрики по нулям)
	ask Float64,
	-- считаем как сумму amount из ticket_flights
	revenue Decimal(18, 2),
	-- количество забронированных мест
	boarded_count UInt64,
	-- произведение boarded_count * distance_km
	rpk Float64
)
-- таблица обязательно должна быть реплицированной
engine = ReplicatedSummingMergeTree(
	-- указываем путь с макросом {shard} для корректного шардирования
    '/clickhouse/tables/{shard}/{database}/{table}', 
    '{replica}'
)
order by (departure_date, departure_airport, arrival_airport, aircraft_code, flight_no);

-- удаляем локальную таблицу на всем кластере
-- Как однако быстро это произошло
--DROP TABLE flight_metrics_local on cluster 'sharded' SYNC;

-- создаем Distirbed таблицу, чтобы при INSERT строки распределялись по ключу шардирования
CREATE TABLE flight_metrics ON CLUSTER 'sharded' AS flight_metrics_local
ENGINE = Distributed(
    'sharded', 
    'nik_polovnikov',         
    'flight_metrics_local', 
    cityHash64(flight_no)
);

-- удаляем Distributed таблицу
DROP TABLE nik_polovnikov.flight_metrics on cluster 'sharded' SYNC;

-- Добавляем проекции, 
-- которые помогут ClickHouse получать данные в уже сагрегированном виде!

-- настраиваем разрешение на использование проекции в агрегирующей таблице
ALTER TABLE flight_metrics_local on cluster 'sharded'
MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';

-- добавляем проекцию
ALTER TABLE flight_metrics_local on cluster 'sharded'
ADD PROJECTION metrics_aircraft_code_projection
(
    SELECT
        departure_date,
        aircraft_code,
        -- суммируем 4 интересующие нас метрики
        sum(ask),
        sum(revenue),
        sum(boarded_count),
        sum(rpk)
    GROUP BY
        departure_date,
        aircraft_code
);

-- материализуем проекцию (если ее не материализовать, 
-- то она будет работать только для новых данных)
ALTER TABLE flight_metrics_local on cluster 'sharded'
MATERIALIZE PROJECTION metrics_aircraft_code_projection;

-- Проверка использования проекции()
EXPLAIN PLAIN
SELECT
    departure_date,
    aircraft_code,
    sum(revenue)
FROM flight_metrics_local
GROUP BY departure_date, aircraft_code;

-- !!!!!!!!!!!!!!!!!!!!!!!!!! Успешно созданные таблицы !!!!!!!!!!!!!!!

-- Создаем материализованное представление, которое 
-- будет вставлять данные из нешардированной локальной flights
-- в Distributed-таблицу flight_metrics.
-- Это необходимо, чтобы данные корректно 
-- распределялись по шардам!

CREATE MATERIALIZED VIEW flights_to_metrics_mv on cluster 'sharded'
TO flight_metrics
AS
SELECT
    -- flight_id, Не верное поле!
    departure_date,
    departure_airport,
    arrival_airport,
    aircraft_code,
    flight_no,
    
    -- получаем на инициализацию только ASK
    ask,
    
    -- остальные поля заполняем нулями
    toDecimal64(0, 2) AS revenue,
    0 AS boarded_count,
    0.0 AS rpk
FROM flights;

-- удаляем старое MV(команда тормозит, но работает!)
DROP TABLE nik_polovnikov.flights_to_metrics_mv on cluster 'sharded' SYNC ;

-- для других таблиц мы создаем материализованные представления
-- которые пишут непосредственно в локальную таблицу,
-- реагируя на INSERT в соответствующие широкие(wide) таблицы

-- берем только revenue из широкой таблицы с билетами
CREATE MATERIALIZED VIEW tickets_to_metrics_mv ON CLUSTER 'sharded'
TO flight_metrics_local -- таблица куда мы складываем данные
AS
SELECT
    departure_date,
    departure_airport,
    arrival_airport,
    aircraft_code,
    flight_no,
    
    0.0 AS ask,
    sum(amount) AS revenue,
    0 AS boarded_count,
    0.0 AS rpk
FROM ticket_flights_wide_local -- таблица откуда мы складываем данные
GROUP BY
    departure_date,
    departure_airport,
    arrival_airport,
    aircraft_code,
    flight_no;
-- За счет широкой таблицы тут никакой JOIN не нужен!!!

DROP TABLE tickets_to_metrics_mv on cluster 'sharded' SYNC ;

CREATE MATERIALIZED VIEW boarding_to_metrics_mv ON CLUSTER 'sharded'
TO flight_metrics_local
AS
SELECT
    departure_date,
    departure_airport,
    arrival_airport,
    aircraft_code,
    flight_no,
    
    0.0 AS ask,
    toDecimal64(0, 2) AS revenue,
    count(boarding_no) AS boarded_count,
    -- очень красивый вариант рассчета RPK!!!
    sum(distance_km) AS rpk
FROM boarding_passes_wide_local
GROUP BY
    departure_date,
    departure_airport,
    arrival_airport,
    aircraft_code,
    flight_no;

DROP TABLE boarding_to_metrics_mv on cluster 'sharded' SYNC ;

    
-- сделаем TRUNCATE для ВСЕХ!! таблиц
-- а далее выполним INSERT в сырые таблицы, чтобы видеть как
-- наполняются сагрегированные таблицы

-- очищаем данные на реплицированном кластере
TRUNCATE TABLE flights ON CLUSTER 'all-replicated';
-- подчищаем данные из Distributed для широких таблиц с шардированного кластера
truncate table ticket_flights_wide on cluster 'sharded' SYNC;
truncate table boarding_passes_wide on cluster 'sharded' SYNC;

truncate table ticket_flights_raw on cluster 'sharded' SYNC;
truncate table boarding_passes_raw on cluster 'sharded' SYNC;


-- вставляем данные в сырые таблицы, чтобы они появились в распределениях через MV
-- Первым делом данные о полетах
-- MV реализует flights_raw -> flights
INSERT INTO flights_raw
SELECT * FROM clickhouse_data.flights;

-- Заполняем данные в сырую таблицу с билетами!
INSERT INTO ticket_flights_raw
select * from clickhouse_data.ticket_flights;

INSERT INTO boarding_passes_raw
select * from clickhouse_data.boarding_passes;
select * from ticket_flights_raw;

-- Получили данные в дистрибутиве связанном с метриками полетов!!!
select * from flight_metrics FINAL;

-- Видимо не все данные еще ко мне приехали!

-- Офигеть, мне зачли это задание на максимум!
    
-- Необходимо проверять, что предыдущая вставка завершилась и доехала до wide таблицы!