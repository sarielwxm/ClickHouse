SET enable_analyzer = 1;
SET union_default_mode = 'DISTINCT';

set enable_global_with_statement = 1;

EXPLAIN SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1;
EXPLAIN (SELECT 1 UNION ALL SELECT 1) UNION ALL SELECT 1;
EXPLAIN SELECT 1 UNION (SELECT 1 UNION ALL SELECT 1);

EXPLAIN SELECT 1 UNION SELECT 1 UNION DISTINCT SELECT 1;
EXPLAIN (SELECT 1 UNION DISTINCT SELECT 1) UNION DISTINCT SELECT 1;
EXPLAIN SELECT 1 UNION DISTINCT (SELECT 1 UNION SELECT 1);

EXPLAIN (SELECT 1 UNION ALL (SELECT 1 UNION ALL (SELECT 1 UNION ALL SELECT 1 UNION SELECT 1))) UNION ALL (((SELECT 1) UNION (SELECT 1 UNION ALL (SELECT 1 UNION ALL (SELECT 1 UNION SELECT 1 ) UNION DISTINCT SELECT 1))));

EXPLAIN (((((((((((((((SELECT 1 UNION ALL SELECT 1) UNION SELECT 1))))))))))))));
EXPLAIN (((((((((((((((((((((((((((((SELECT 1 UNION SELECT 1)))))))))))))))))))))))))))));
EXPLAIN (((((((((((((((((((((((((((((SELECT 1 UNION SELECT 1)))))))))))))))))))))))))))));

SET union_default_mode='ALL';

EXPLAIN SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1;
EXPLAIN (SELECT 1 UNION ALL SELECT 1) UNION ALL SELECT 1;
EXPLAIN SELECT 1 UNION (SELECT 1 UNION ALL SELECT 1);

EXPLAIN SELECT 1 UNION SELECT 1 UNION DISTINCT SELECT 1;
EXPLAIN (SELECT 1 UNION DISTINCT SELECT 1) UNION DISTINCT SELECT 1;
EXPLAIN SELECT 1 UNION DISTINCT (SELECT 1 UNION SELECT 1);

EXPLAIN (SELECT 1 UNION ALL (SELECT 1 UNION ALL (SELECT 1 UNION ALL SELECT 1 UNION SELECT 1))) UNION ALL (((SELECT 1) UNION (SELECT 1 UNION ALL (SELECT 1 UNION ALL (SELECT 1 UNION SELECT 1 ) UNION DISTINCT SELECT 1))));

EXPLAIN (((((((((((((((SELECT 1 UNION ALL SELECT 1) UNION SELECT 1))))))))))))));
EXPLAIN (((((((((((((((((((((((((((((SELECT 1 UNION SELECT 1)))))))))))))))))))))))))))));
EXPLAIN (((((((((((((((((((((((((((((SELECT 1 UNION SELECT 1)))))))))))))))))))))))))))));
