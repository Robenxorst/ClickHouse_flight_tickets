-- 1 запрос: Хотим узнать о задержках рейсов (OTP - насколько рейс задержался);
-- Для этого используем таблицу flight_statuses;

WITH
    dateDiff('minute', f.scheduled_departure, fs.actual_departure) AS delay_minutes
	
select
    concat(f.departure_airport, ' - ', f.arrival_airport) AS route,
    dictGetString('airports_dict', 'city', f.departure_airport) AS dep_city,
    dictGetString('airports_dict', 'city', f.arrival_airport) AS arr_city,
    count() AS total_flights,
    -- Процент рейсов, вылетевших вовремя. Считаем, что рейс вылетел вовремя, если задержка не превышает 15 мин.
    countIf(delay_minutes <= 15) / count() AS otp,
    -- средняя задержка (только > 0)
    avgIf(delay_minutes, delay_minutes > 0) AS avg_delay,
    -- отменённые рейсы
    countIf(fs.status = 'Cancelled') AS cancelled_flights
FROM flights f
LEFT JOIN flight_statuses fs
    ON f.flight_id = fs.flight_id
-- смотрим данные отдельно за сентябрь
WHERE f.departure_date >= '2015-11-01'
  AND f.departure_date < '2016-09-01'
  and fs.status = 'Cancelled'
GROUP BY
    route,
    dep_city,
    arr_city
ORDER by
-- сортируем по возрастанию OTP, 
-- чтобы все самое проблемное было вверху
    otp ASC;



-- Запрос 2: Общий SQL-запрос к витрине со всеми метриками

WITH
    dateDiff('minute', f.scheduled_departure, fs.actual_departure) AS delay_minutes,

    sum(m.revenue) / sum(m.rpk) AS yield,
    sum(m.rpk) / sum(m.ask) AS load_factor,
    sum(m.revenue) / sum(m.ask) AS rask

SELECT
    concat(f.departure_airport, ' - ', f.arrival_airport) AS route,

    dictGetString('airports_dict', 'city', f.departure_airport) AS dep_city,
    dictGetString('airports_dict', 'city', f.arrival_airport) AS arr_city,

    -- бизнес метрики
    sum(m.ask) AS total_ask,
    sum(m.boarded_count) AS total_passengers,
    sum(m.revenue) AS total_revenue,

    yield,
    load_factor,
    rask,

    -- performance
    multiIf(
        yield >= 11 AND load_factor >= 0.7, 'Healthy',
        yield >= 11 AND load_factor < 0.7, 'Demand Issue',
        yield < 11 AND load_factor >= 0.7, 'Pricing Issue',
        'Poor'
    ) AS performance,

    -- ops метрики
    count() AS total_flights,
    countIf(delay_minutes <= 15) / count() AS otp,
    avgIf(delay_minutes, delay_minutes > 0) AS avg_delay,
    countIf(fs.status = 'Cancelled') AS cancelled_flights

FROM flights f

LEFT JOIN flight_statuses fs
    ON f.flight_id = fs.flight_id

LEFT JOIN flight_metrics m
    ON f.flight_no = m.flight_no
   AND f.departure_date = m.departure_date
   AND f.departure_airport = m.departure_airport
   AND f.arrival_airport = m.arrival_airport

WHERE f.departure_date >= '2016-09-01'
  AND f.departure_date < '2016-10-01'

GROUP BY
    route,
    dep_city,
    arr_city

HAVING total_revenue > 0

ORDER BY
    otp ASC;
