SELECT
    ma.symbol,
    ma.date,
    ma.close,
    ma.moving_average_7d,
    ma.moving_average_30d,
    rc.rsi,
    (pm.close - pm.prev_close_14) / pm.prev_close_14 * 100 AS price_momentum_14d
FROM 
    (SELECT
        symbol,
        date,
        close,
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_average_7d,
        AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS moving_average_30d
    FROM DEV.ANALYTICS.STOCK_ABSTRACT_VIEW) AS ma
JOIN 
    (SELECT
        symbol,
        date,
        CASE 
            WHEN avg_loss = 0 THEN 100
            WHEN avg_gain = 0 THEN 0
            ELSE 100 - (100 / (1 + (avg_gain / avg_loss))) 
        END AS rsi
    FROM (
        SELECT
            symbol,
            date,
            AVG(CASE WHEN delta > 0 THEN delta ELSE 0 END) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
            AVG(CASE WHEN delta < 0 THEN ABS(delta) ELSE 0 END) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
        FROM (
            SELECT
                symbol,
                date,
                close - LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS delta
            FROM DEV.ANALYTICS.STOCK_ABSTRACT_VIEW
        )
    )) AS rc ON ma.symbol = rc.symbol AND ma.date = rc.date
JOIN 
    (SELECT
        symbol,
        date,
        close,
        LAG(close, 14) OVER (PARTITION BY symbol ORDER BY date) AS prev_close_14
    FROM DEV.ANALYTICS.STOCK_ABSTRACT_VIEW) AS pm ON ma.symbol = pm.symbol AND ma.date = pm.date
WHERE pm.prev_close_14 IS NOT NULL
