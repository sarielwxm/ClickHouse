SELECT 1 SETTINGS max_execution_time=NaN; -- { serverError 72 }
SELECT 1 SETTINGS max_execution_time=Infinity; -- { serverError 72 };
SELECT 1 SETTINGS max_execution_time=-Infinity; -- { serverError 72 };
SELECT 1 SETTINGS max_execution_time=-0.5; -- { serverError 72 };
SELECT 1 SETTINGS max_execution_time=-0.000000000001; -- { serverError 72 };
SELECT 1 SETTINGS max_execution_time=-0.0; -- { serverError 72 };
SELECT 1 SETTINGS max_execution_time=0.0;
SELECT 1 SETTINGS max_execution_time=10.5;
SELECT 1 SETTINGS max_execution_time=10;
