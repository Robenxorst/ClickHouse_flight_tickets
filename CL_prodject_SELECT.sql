-- Задание 7.

-- в качестве временных рамок используем сентябрь 2016
select 
	-- получаем маршрут, разделенный тире
	concatWithSeparator(' - ', departure_airport, arrival_airport) AS route,
	
	-- не на всех шардах есть интересующие нас словари
    dictGet('nik_polovnikov.airports_dict', 'city', departure_airport) as dep_city ,
	dictGet('nik_polovnikov.airports_dict', 'city', arrival_airport) as arr_city ,
	ROUND(sum(ask), 0) AS total_ask,
    sum(boarded_count) AS total_passengers,
    sum(revenue) AS total_revenue,

    ROUND(sum(revenue) / sum(rpk), 2) AS yield,
    ROUND(sum(rpk) / sum(ask), 4) AS load_factor,
    ROUND(sum(revenue) / sum(ask), 2) AS rask,

    multiIf(
        (sum(revenue) / sum(rpk) >= 11) AND (sum(rpk) / sum(ask) >= 0.7), 'Healthy',
        (sum(revenue) / sum(rpk) >= 11) AND (sum(rpk) / sum(ask) < 0.7), 'Demand Issue',
        (sum(revenue) / sum(rpk) < 11) AND (sum(rpk) / sum(ask) >= 0.7 - 1e-6), 'Pricing Issue',
        'Poor'
    ) AS performance

FROM flight_metrics

where departure_date >= '2016-09-01' and departure_date < '2016-10-01'
GROUP BY
    route,
    departure_airport,
    arrival_airport

HAVING total_revenue > 0
-- (rask - 7.7) * sum(ask) совпадает с total_revenue - 7.7 * sum(ask)
order by total_revenue - 7.7 * sum(ask), route;

-- идея со словарями-таблицами на всем шарде оказалась успешной!