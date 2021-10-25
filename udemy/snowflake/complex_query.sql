USE WAREHOUSE AS_WH;

SELECT * FROM
    (
        SELECT RANK() OVER ( ORDER BY COL1 DESC ) AS RNK FROM
            (
                SELECT UUID_STRING() AS COL1 FROM large_table
            ) A
    )
WHERE RNK < 100;
