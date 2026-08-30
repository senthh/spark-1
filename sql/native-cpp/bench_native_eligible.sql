-- Native-eligible primitive workload (int/long only).
-- Used to show Spark 4.2.0 JVM vs Native SQL on a plan the C++ engine can take.

DROP TABLE IF EXISTS n1;
DROP TABLE IF EXISTS n2;
CREATE TABLE n1 AS SELECT id, id % 1024 AS k, id * 3 AS v FROM range(0, 2000000);
CREATE TABLE n2 AS SELECT id, id % 256 AS k, id * 7 AS v FROM range(0, 200000);

-- filter + project
SELECT k, v + k AS s FROM n1 WHERE k > 500;

-- hash agg
SELECT k, sum(v) AS s, count(*) AS c FROM n1 WHERE v > 16 GROUP BY k;

-- hash join
SELECT a.k, sum(a.v + b.v) AS s
FROM n1 a JOIN n2 b ON a.k = b.k
WHERE a.v > 100
GROUP BY a.k;
