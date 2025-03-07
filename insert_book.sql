
\set i 20
BEGIN;
    DO $$
    DECLARE
        current_i INTEGER := :i;
    BEGIN
        WHILE current_i <= 100 LOOP
            INSERT INTO Books (book_id, title, author_id, publish_year, genre)
            VALUES (current_i, '1984', 1, 1949, 'Dystopian');
    
            current_i := current_i + 1;
        END LOOP;
    END $$;
COMMIT;