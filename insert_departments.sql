-- insert_departments.sql

\set i 300
BEGIN;
    DO $$
    DECLARE
        current_i INTEGER := :i;
    BEGIN
        WHILE current_i <= 350 LOOP
            INSERT INTO departments (department_id, department_name)
            VALUES (current_i, 'Department ' || current_i);
            current_i := current_i + 1;
        END LOOP;
    END $$;
COMMIT;