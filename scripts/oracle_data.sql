SET ECHO OFF
SET FEEDBACK OFF

INSERT INTO system.all_types VALUES (1, 100, 1000000, 50, 1.5, 2.5, 123.45, 'test', 'test text', 1, SYSDATE, SYSDATE, NULL);
INSERT INTO system.all_types VALUES (2, 200, 2000000, 100, 3.5, 4.5, 456.78, 'test2', 'test2 text', 0, SYSDATE, SYSDATE, NULL);
INSERT INTO system.all_types VALUES (3, 300, 3000000, 150, 5.5, 6.5, 789.12, 'test3', 'test3 text', 1, SYSDATE, SYSDATE, NULL);
COMMIT;

INSERT INTO system.nullable_test VALUES (1, 100, 'test1', 100.50, SYSDATE, 1);
INSERT INTO system.nullable_test VALUES (2, NULL, NULL, NULL, NULL, NULL);
INSERT INTO system.nullable_test VALUES (3, 300, 'test3', 300.75, SYSDATE, 0);
COMMIT;

INSERT INTO system.huge_table VALUES (1, 'name_0001', 10.00, 1, SYSDATE);
INSERT INTO system.huge_table VALUES (2, 'name_0002', 20.00, 0, SYSDATE);
INSERT INTO system.huge_table VALUES (3, 'name_0003', 30.00, 1, SYSDATE);
COMMIT;

SELECT 'all_types' as tbl, COUNT(*) as cnt FROM system.all_types
UNION ALL
SELECT 'nullable_test', COUNT(*) FROM system.nullable_test
UNION ALL
SELECT 'huge_table', COUNT(*) FROM system.huge_table;
EXIT