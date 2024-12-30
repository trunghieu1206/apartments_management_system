-- REQUIREMENTS:
-- create a database named `apartments_project` 
-- connect to this database and then run the sql script

-- SOME NOTES:
    -- when declaring a column as UNIQUE, a BTREE index is automatically created for that column

-------------------------- TABLES ---------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS tenants, landlords, apartments, requests, contracts, bills, payment_details, rating;

CREATE TABLE tenants (
    tenant_id INT PRIMARY KEY,
    email VARCHAR(50) NOT NULL,
    password VARCHAR(50) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    phone VARCHAR(50)
);

CREATE TABLE landlords (
    landlord_id INT PRIMARY KEY,
    email VARCHAR(50) NOT NULL,
    password VARCHAR(50) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    phone VARCHAR(50)
);

CREATE TABLE apartments (
    apartment_id INT PRIMARY KEY,
    address VARCHAR(50) NOT NULL,
    size INT,
    bedrooms INT NOT NULL,
    bathrooms INT NOT NULL,
    kitchen VARCHAR(3),
    air_conditioner VARCHAR(3),
    tv VARCHAR(3),
    landlord_id INT NOT NULL
);

CREATE TABLE requests (
    request_id INT PRIMARY KEY,
    tenant_id INT,
    apartment_id INT,
    request_date DATE,
    start_month CHAR(7) NOT NULL, -- for e.g: '2024-12' or '2025-01' 
    duration INT NOT NULL
);

CREATE TABLE contracts (
    contract_id INT PRIMARY KEY,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    rent_amount INT NOT NULL,
    request_id INT NOT NULL
);

CREATE TABLE bills (
    bill_id SERIAL PRIMARY KEY, -- bill_id is automatically incremented
    contract_id INT,
    month CHAR(7),
    price INT NOT NULL
);

CREATE TABLE payment_details (
    bill_id INT,
    tenant_id INT,
    payment_date DATE NOT NULL
);

CREATE TABLE rating (
    tenant_id INT,
    apartment_id INT,
    score INT
);

--------------------- PK, FK, UNIQUE CONSTRAINTS --------------------
---------------------------------------------------------------------

-- `tenants` table
ALTER TABLE tenants
ADD CONSTRAINT tenants_unique_email UNIQUE(email);

ALTER TABLE tenants 
ADD CONSTRAINT tenants_unique_phone UNIQUE(phone);

-- `landlords` table
ALTER TABLE landlords
ADD CONSTRAINT landlords_unique_email UNIQUE(email);

ALTER TABLE landlords 
ADD CONSTRAINT landlords_unique_phone UNIQUE(phone);

-- `apartments` table
ALTER TABLE apartments
ADD CONSTRAINT apartments_unique_address UNIQUE(address);

ALTER TABLE apartments
ADD CONSTRAINT apartments_fk_landlords FOREIGN KEY (landlord_id)
REFERENCES landlords(landlord_id)
ON UPDATE CASCADE 
ON DELETE CASCADE;

-- `requests` table
-- 
ALTER TABLE requests 
ADD CONSTRAINT requests_fk_tenants FOREIGN KEY (tenant_id)
REFERENCES tenants(tenant_id)
ON UPDATE CASCADE -- if tenant_id in `tenants` gets updated this will also get updated
ON DELETE SET NULL;

ALTER TABLE requests 
ADD CONSTRAINT requests_fk_apartments FOREIGN KEY (apartment_id)
REFERENCES apartments(apartment_id)
ON UPDATE CASCADE -- if aparment_id in `apartments` gets updated this will also get updated
ON DELETE SET NULL;

-- duration has to be greater than 3
ALTER TABLE requests 
ADD CONSTRAINT requests_check_duration 
CHECK (duration >= 3);

-- start_month has to be after request_date
ALTER TABLE requests 
ADD CONSTRAINT requests_check_start_month_after_request_date 
CHECK (TO_DATE(start_month || '-01', 'YYYY-MM-DD') > request_date);

-- start_month must be within 1 year of request_date
ALTER TABLE requests
ADD CONSTRAINT requests_check_start_month_within_1_year
CHECK (TO_DATE(start_month || '-01', 'YYYY-MM-DD') <= request_date + INTERVAL '1 year');

-- `contracts` table
--
ALTER TABLE contracts 
ADD CONSTRAINT contracts_fk_requests FOREIGN KEY (request_id)
REFERENCES requests(request_id)
ON UPDATE CASCADE -- 
ON DELETE SET NULL; 

-- `bills` table
--
ALTER TABLE bills
ADD CONSTRAINT bills_fk_contracts FOREIGN KEY (contract_id)
REFERENCES contracts(contract_id)
ON UPDATE CASCADE -- if contract_id in `contracts` gets updated this will also get updated
ON DELETE CASCADE;

ALTER TABLE bills
ADD CONSTRAINT bills_unique_contract_id_month UNIQUE (contract_id, month);

-- `payment_details` table
--
ALTER TABLE payment_details
ADD CONSTRAINT payment_details_fk_bills FOREIGN KEY (bill_id)
REFERENCES bills(bill_id)
ON UPDATE CASCADE -- if bill_id gets updated in `bills` this will get updated
ON DELETE CASCADE;  -- if bill is deleted payment_details also get deleted

ALTER TABLE payment_details
ADD CONSTRAINT payment_details_fk_tenants FOREIGN KEY (tenant_id)
REFERENCES tenants(tenant_id)
ON UPDATE CASCADE -- if tenant_id in `tenant` gets updated this will also get updated
ON DELETE SET NULL;

ALTER TABlE payment_details 
ADD CONSTRAINT payment_details_unique_bill_id_tenant_id UNIQUE (bill_id, tenant_id);

-- `rating` table
--
ALTER TABLE rating 
ADD CONSTRAINT rating_fk_tenants FOREIGN KEY (tenant_id)
REFERENCES tenants(tenant_id)
ON UPDATE CASCADE
ON DELETE SET NULL;

ALTER TABLE rating 
ADD CONSTRAINT rating_fk_apartments FOREIGN KEY (apartment_id)
REFERENCES apartments(apartment_id)
ON UPDATE CASCADE
ON DELETE SET NULL;

ALTER TABlE rating 
ADD CONSTRAINT rating_unique_tenant_id_apartment_id UNIQUE (tenant_id, apartment_id);

---------------------- TRIGGER CONSTRAINTS ------------------------
-------------------------------------------------------------------
-- NOTE that when a batch of records are to be added to table and one of 
-- them violates the trigger constraint then the operation will be aborted,
-- which means no records are added at all


-- `requests` table
-- insert constraints: if tenants want to create a new request
--
CREATE OR REPLACE FUNCTION tf_bf_insert_on_requests()
RETURNS TRIGGER AS $$
DECLARE
    v_request_start_date DATE;
    v_request_end_date DATE;
BEGIN

    -- check if tenant has already requested for this apartment in that month
    IF EXISTS (
        SELECT 1 
        FROM requests 
        WHERE tenant_id = NEW.tenant_id
            AND apartment_id = NEW.apartment_id
            AND TO_CHAR(request_date, 'YYYY-MM') = TO_CHAR(NEW.request_date, 'YYYY-MM')
    ) THEN 
        RAISE EXCEPTION 'Request already made for this apartment in the specified month';
    END IF;

    -- check if there is a contract on the requested apartment where it overlaps with [start_month:(start_month + duration)]
    v_request_start_date := TO_DATE(NEW.start_month || '-01', 'YYYY-MM-DD');
    v_request_end_date := v_request_start_date + (NEW.duration || ' months')::INTERVAL - INTERVAL '1 day';

    IF EXISTS (
        SELECT 1
        FROM contracts C
        JOIN requests R ON R.request_id = C.request_id
        WHERE R.apartment_id = NEW.apartment_id
          AND C.start_date <= v_request_end_date
          AND C.end_date >= v_request_start_date
    ) THEN
        RAISE EXCEPTION 'Cannot accept request %, apartment is being rented between the specified period. Tenant ID: %, Apartment ID: %', NEW.request_id, NEW.tenant_id, NEW.apartment_id;
    END IF;

    -- if every conditions are satisfied then proceed inserting
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bf_insert_on_requests
BEFORE INSERT ON requests
FOR EACH ROW
EXECUTE PROCEDURE tf_bf_insert_on_requests();

-- `requests` table
-- contraint: if tenant wants to cancel a created request 
--
CREATE OR REPLACE FUNCTION tf_bf_delete_on_requests()
RETURNS TRIGGER AS $$
BEGIN 

    -- check if there is already a contract formed
    IF EXISTS (
        SELECT 1 
        FROM contracts C
        JOIN requests R ON R.request_id = C.request_id
        WHERE R.request_id = OLD.request_id
    ) THEN
        RAISE EXCEPTION 'Cannot cancel request, contract has been formed';
    END IF;

    -- if condition satisfies then proceed
    RETURN OLD;

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bf_delete_on_requests
BEFORE DELETE ON requests
FOR EACH ROW 
EXECUTE PROCEDURE tf_bf_delete_on_requests();

-- `contracts` table
-- insert constraint for landlords: when landlord accepts a request
-- note: insert query will insert 3 fields (contract_id, rent_amount, request_id)
-- the other 2 columns: start_date and end_date will be auto calculated
--
CREATE OR REPLACE FUNCTION tf_bf_insert_on_requests()
RETURNS TRIGGER AS $$
DECLARE
    v_request_start_date DATE;
    v_request_end_date DATE;
BEGIN
    -- check if tenant has already requested for this apartment in that month
    IF EXISTS (
        SELECT 1 
        FROM requests 
        WHERE tenant_id = NEW.tenant_id
            AND apartment_id = NEW.apartment_id
            AND TO_CHAR(request_date, 'YYYY-MM') = TO_CHAR(NEW.request_date, 'YYYY-MM')
    ) THEN 
        RAISE EXCEPTION 'Request already made for this apartment in the specified month';
    END IF;
    -- check if there is a contract on the requested apartment where it overlaps with [start_month:(start_month + duration)]
    v_request_start_date := TO_DATE(NEW.start_month || '-01', 'YYYY-MM-DD');
    v_request_end_date := v_request_start_date + (NEW.duration || ' months')::INTERVAL - INTERVAL '1 day';
    IF EXISTS (
        SELECT 1
        FROM contracts C
        JOIN requests R ON R.request_id = C.request_id
        WHERE R.apartment_id = NEW.apartment_id
          AND C.start_date <= v_request_end_date
          AND C.end_date >= v_request_start_date
    ) THEN
        RAISE EXCEPTION 'Cannot accept request %, apartment is being rented between the specified period. Tenant ID: %, Apartment ID: %', NEW.request_id, NEW.tenant_id, NEW.apartment_id;
    END IF;
    -- if every conditions are satisfied then proceed inserting
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bf_insert_on_contracts
BEFORE INSERT ON contracts
FOR EACH ROW
EXECUTE PROCEDURE tf_bf_insert_on_contracts();

-- `contracts` table
-- update constraint
-- (when tenant wants to end contract sooner, or when landlord needs to terminate contract)
-- 
CREATE OR REPLACE FUNCTION tf_bf_update_on_contracts()
RETURNS TRIGGER AS $$
BEGIN

    -- not yet implement


END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bf_update_on_contracts
BEFORE UPDATE ON contracts
FOR EACH ROW
EXECUTE PROCEDURE tf_bf_update_on_contracts();

-- `rating` table
-- insert constraint: when tenants want to rate an apartment
CREATE OR REPLACE FUNCTION tf_bf_insert_on_rating()
RETURNS TRIGGER AS $$
BEGIN 

    -- check if tenant has rented the apartment before or is currently renting
    IF NOT EXISTS (
        SELECT 1 
        FROM contracts C 
        JOIN requests R ON R.request_id = C.request_id
        WHERE R.tenant_id = NEW.tenant_id
            AND R.apartment_id = NEW.apartment_id
    ) THEN 
        RAISE EXCEPTION 'Cannot rate, tenant % has not rented apartment %', NEW.tenant_id, NEW.apartment_id;
    END IF;

    -- if ok 
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bf_insert_on_rating
BEFORE INSERT ON rating
FOR EACH ROW
EXECUTE PROCEDURE tf_bf_insert_on_rating();

---------------------- FUNCTIONS ----------------------------------
-------------------------------------------------------------------
-- auto add bill function, can be called using pg_cron extension to auto check for 5th day
CREATE OR REPLACE FUNCTION generate_bills()
RETURNS VOID AS $$
DECLARE 
    v_contract_id INT;
    v_rent_amount INT;
BEGIN 
    -- select all contracts which are still active
    -- then add into bill

    -- 1st approach: using for loop
    -- FOR v_contract_id IN (
    --     SELECT contract_id 
    --     FROM contracts 
    --     WHERE end_date > CURRENT_DATE
    -- ) LOOP
    --     -- get base rent_amount
    --     SELECT INTO v_rent_amount rent_amount 
    --     FROM contracts 
    --     WHERE contract_id = v_contract_id;

    --     -- insert into bill table
    --     INSERT INTO bills (contract_id, month, price)
    --     VALUES (v_contract_id, EXTRACT(MONTH FROM CURRENT_DATE), v_rent_amount)
    -- END LOOP;

    -- 2nd approach: more optimized
    INSERT INTO bills (contract_id, month, price)
    SELECT contract_id, EXTRACT(MONTH FROM CURRENT_DATE), rent_amount
    FROM contracts
    WHERE end_date > CURRENT_DATE;


END;
$$ LANGUAGE plpgsql;

-- function to generate all bills based on existing contracts in the db
CREATE OR REPLACE FUNCTION generate_all_bills()
RETURNS VOID AS $$
DECLARE
    v_loop RECORD;
    v_loop_date DATE;
BEGIN 

    -- loop through all contracts 
    FOR v_loop IN (
        SELECT contract_id, start_date, end_date, rent_amount
        FROM contracts
    ) LOOP 
        -- get date
        v_loop_date := v_loop.start_date + INTERVAL '4 days';

        -- generate bills for each month from start_date to end_date 
        -- note that we only gen bills for 5-th day of the month which is before CURRENT_DATE
        WHILE (v_loop_date <= v_loop.end_date AND v_loop_date <= CURRENT_DATE) LOOP
            -- insert a record into `bills` table
            INSERT INTO bills (contract_id, month, price)
            VALUES (v_loop.contract_id, TO_CHAR(v_loop_date, 'YYYY-MM'), v_loop.rent_amount)
            ON CONFLICT (contract_id, month) DO NOTHING; -- in case this function is called multiple times

            RAISE NOTICE 'values inserted: %, %, %', v_loop.contract_id, TO_CHAR(v_loop_date, 'YYYY-MM'), v_loop.rent_amount;

            -- increment
            v_loop_date := v_loop_date + INTERVAL '1 month';
        END LOOP;   
    END LOOP;

END;
$$ LANGUAGE plpgsql;

-- function to generate all payment details based on existing bills in db
-- note: only generate records until CURRENT_DATE's month
CREATE OR REPLACE FUNCTION generate_all_payment_details()
RETURNS VOID AS $$
DECLARE
    v_loop RECORD;
    v_random_day INT;
    v_payment_date DATE;
    v_month_end_date DATE;
BEGIN 

    -- loop through all bills 
    FOR v_loop IN (
        SELECT B.bill_id, R.tenant_id, B.month
        FROM bills B
        JOIN contracts C ON C.contract_id = B.contract_id
        JOIN requests R ON R.request_id = C.request_id
    ) LOOP 
        -- get the last day of the month
        v_month_end_date := TO_DATE(v_loop.month || '-01', 'YYYY-MM-DD') + INTERVAL '1 month - 1 day';

        -- only add payment details until CURRENT_DATE's previous month
        IF v_month_end_date < CURRENT_DATE THEN
            -- generate a random day after the 5th of the month
            v_random_day := 5 + FLOOR(random() * (DATE_PART('days', v_month_end_date)::INT - 5 + 1));

            -- calculate the payment_date
            v_payment_date := TO_DATE(v_loop.month || '-01', 'YYYY-MM-DD') + (v_random_day || ' days')::INTERVAL;

            -- insert into payment_details table
            INSERT INTO payment_details (bill_id, tenant_id, payment_date)
            VALUES (v_loop.bill_id, v_loop.tenant_id, v_payment_date)
            ON CONFLICT (bill_id, tenant_id) DO NOTHING;
        END IF;

    END LOOP;

END;
$$ LANGUAGE plpgsql;


-- function to generate all rating based on existing contracts in db
-- note: only generate records
CREATE OR REPLACE FUNCTION generate_all_rating()
RETURNS VOID AS $$
DECLARE
    v_loop RECORD;
    v_random_score INT;
BEGIN 

    -- loop through all contracts to get tenant_id and apartment_id
    FOR v_loop IN (
        SELECT R.tenant_id, R.apartment_id
        FROM contracts C 
        JOIN requests R ON R.request_id = C.request_id
    ) LOOP 

        -- generate a random score between 1 and 5
        v_random_score := ROUND(RANDOM()*(5 - 1) + 1);

        -- insert a record into rating table
        INSERT INTO rating (tenant_id, apartment_id, score)
        VALUES (v_loop.tenant_id, v_loop.apartment_id, v_random_score)
        ON CONFLICT (tenant_id, apartment_id) DO NOTHING;

    END LOOP;

END;
$$ LANGUAGE plpgsql;

------------------ POPULATING DATA to tables ----------------------
-------------------------------------------------------------------
-- NOTE: used your own path to sql files
-- Data is mocked from 2020-01-01 to 2024-12-18 (for request_date in requests table)
-- \i /Users/hieuhoang/Desktop/Projects/apartments_management_system/data/tenants/tenants.sql
  
-- \i /Users/hieuhoang/Desktop/Projects/apartments_management_system/data/landlords/landlords.sql

-- \i /Users/hieuhoang/Desktop/Projects/apartments_management_system/data/apartments/apartments.sql

-- \i /Users/hieuhoang/Desktop/Projects/apartments_management_system/data/requests/requests.sql;

-- \i /Users/hieuhoang/Desktop/Projects/apartments_management_system/data/contracts/contracts.sql

-- SELECT generate_all_bills();

-- SELECT generate_all_payment_details();

-- SELECT generate_all_rating();

-------------------------------------------------------------------
-------------- QUERIES by team members ----------------------------
-------------------------------------------------------------------

-------------------------------------------------------------------
-------------- Hoang Trung Hieu -----------------------------------
-------------------------------------------------------------------
-------------------------------------------------------------------
-- 1: view currently available apartments
--
EXPLAIN ANALYZE SELECT apartment_id
FROM apartments A
EXCEPT 
SELECT apartment_id 
FROM requests R 
JOIN contracts C ON C.request_id = R.request_id
WHERE C.start_date <= CURRENT_DATE
    AND C.end_date >= CURRENT_DATE;
-- cost with no additional index: 472.43

-- try with composite index
-- note that end_date is the leading column in this composite index
CREATE INDEX idx_contracts_start_end_date ON contracts(end_date, start_date);
--> cost: 431.03
DROP INDEX idx_contracts_start_end_date;

-- try with individual index
-- CREATE INDEX idx_contracts_start_date ON contracts(start_date);
CREATE INDEX idx_contracts_end_date ON contracts(end_date);
--> cost: 424.18
-- explain why index on start_date doesn't prove effective: condition 
-- start_date <= CURRENT_DATE returns a lot of records which satisfy,
-- when a condition matches large portion of rows, the optimizer 
-- might decide to do a seq scan instead.

-- with additional index on contracts.request_is
CREATE INDEX idx_contracts_request_id ON contracts(request_id);
-- cost: 424.18 --> no improvement at all -> bad

DROP INDEX idx_contracts_end_date;
DROP INDEX idx_contracts_request_id;

-- 2nd method: more optimized
EXPLAIN ANALYZE SELECT apartment_id
FROM apartments A
WHERE apartment_id NOT IN (
    SELECT apartment_id 
    FROM requests R
    JOIN contracts C ON C.request_id = R.request_id
    WHERE C.start_date <= CURRENT_DATE
        AND C.end_date >= CURRENT_DATE
);
-- cost without additional index: 410.43

-- try with index on contracts.end_date
CREATE INDEX idx_contracts_end_date ON contracts (end_date);
--> cost: 362.19

-- with addtional index on contracts.request_id
CREATE INDEX idx_contracts_request_id ON contracts(request_id);
-- cost: 362.19 --> no improvement at all -> bad
-- reasons: maybe the contracts table is too small? (only 4404 records)

DROP INDEX idx_contracts_end_date;
DROP INDEX idx_contracts_request_id;

-- 3rd method:
EXPLAIN ANALYZE SELECT A.apartment_id
FROM apartments A
WHERE NOT EXISTS (
    SELECT 1
    FROM requests R
    JOIN contracts C ON C.request_id = R.request_id
    WHERE C.start_date <= CURRENT_DATE
      AND C.end_date >= CURRENT_DATE
      AND R.apartment_id = A.apartment_id
);
-- cost: 445.33

CREATE INDEX idx_requests_apartment_id ON requests(apartment_id);
CREATE INDEX idx_contracts_end_date ON contracts(end_date);
-- cost: 397.09

DROP INDEX idx_requests_apartment_id;
DROP INDEX idx_contracts_end_date;

---------------------------------------------------------------------
-- 6: view payment history
-- this will return a set of records where each row contains bill_id and payment_date
--
CREATE OR REPLACE FUNCTION check_payment_history(input_tenant_id INT)
RETURNS TABLE(o_bill_id INT, o_payment_date DATE) AS $$
BEGIN

    -- select all payment history made by this tenant
    RETURN QUERY
    SELECT bill_id, payment_date 
    FROM payment_details
    WHERE tenant_id = input_tenant_id;

END;
$$ LANGUAGE plpgsql;

SELECT * FROM check_payment_history(1);

-- inner query alysis
EXPLAIN ANALYZE SELECT bill_id, payment_date 
FROM payment_details
WHERE tenant_id = 1;
-- cost without index: 837.61

-- B-tree index on payment_details.tenant_id:
CREATE INDEX idx_payment_details_tenant_id ON payment_details(tenant_id);
-- cost with index: 112.94 


---------------------------------------------------------------------
-- 8: view current active contract of a tenant
-- NOTE: SETOF is a keyword used define a set of rows as the return type
--
CREATE OR REPLACE FUNCTION view_current_active_contract(input_tenant_id INT)
RETURNS SETOF INT AS $$
BEGIN 

    RETURN QUERY
    SELECT contract_id
    FROM contracts
    JOIN requests USING(request_id)
    WHERE tenant_id = input_tenant_id
        AND start_date <= CURRENT_DATE
        AND end_date >= CURRENT_DATE;

END;
$$ LANGUAGE plpgsql;

SELECT view_current_active_contract(1);

-- inner query analysis
EXPLAIN ANALYZE SELECT contract_id
FROM contracts
JOIN requests USING(request_id)
WHERE tenant_id = 2
    AND start_date <= CURRENT_DATE
    AND end_date >= CURRENT_DATE;
-- cost without index: 305.06

-- try with index on contracts.end_date
CREATE INDEX idx_contracts_end_date ON contracts (end_date);
-- cost: 256.82

-- with additional index on contracts.request_id
CREATE INDEX idx_contracts_request_id ON contracts(request_id);
-- cost: 247.77 -> performance does not improve very much -> index is 
-- not very beneficial due to contracts table is small (4404 records only)

-- with additional index on requests.tenant_id
CREATE INDEX idx_requests_tenant_id ON requests (tenant_id);
-- cost: 93.60 -> index proves beneficial because for each tenant_id
-- we can determine a small number of matching rows

DROP INDEX idx_contracts_request_id;
DROP INDEX idx_requests_tenant_id;
DROP INDEX idx_contracts_end_date;

---------------------------------------------------------------------
-- 16: view request history
--
CREATE OR REPLACE FUNCTION view_request_history(input_tenant_id INT)
RETURNS SETOF INT AS $$
BEGIN 

    RETURN QUERY
    SELECT request_id
    FROM requests 
    WHERE tenant_id = input_tenant_id;

END;
$$ LANGUAGE plpgsql;

SELECT view_request_history(1);

-- inner query analysis
EXPLAIN SELECT request_id
FROM requests 
WHERE tenant_id = 1;
-- cost without index: 184.89

-- index on requests.tenant_id
CREATE INDEX idx_requests_tenant_id ON requests(tenant_id);
-- cost: 30.72 -> beneficial use of index since for a specific tenant_id
-- we can determine a small number of matching rows in requests table

DROP INDEX idx_requests_tenant_id;

---------------------------------------------------------------------
-- 21: view all owned apartments
--
CREATE OR REPLACE FUNCTION view_owned_apartments(input_landlord_id INT)
RETURNS SETOF INT AS $$
BEGIN 

    RETURN QUERY
    SELECT apartment_id 
    FROM apartments 
    WHERE landlord_id = input_landlord_id;

END;
$$ LANGUAGE plpgsql;

SELECT view_owned_apartments(1);

-- inner query analysis
EXPLAIN ANALYZE SELECT apartment_id 
FROM apartments 
WHERE landlord_id = 1;
-- cost without index: 68.45

-- index on apartment.landlord_id
CREATE INDEX idx_apartments_landlord_id ON apartments(landlord_id);
-- cost: 13.54

DROP INDEX idx_apartments_landlord_id;

---------------------------------------------------------------------
-- 31: view tenants basic information
CREATE OR REPLACE FUNCTION view_tenant_information(input_tenant_id INT)
RETURNS TABLE(o_first_name VARCHAR(50), o_last_name VARCHAR(50), o_email VARCHAR(50), o_phone VARCHAR(50)) AS $$
BEGIN 

    RETURN QUERY 
    SELECT first_name, last_name, email, phone
    FROM tenants 
    WHERE tenant_id = input_tenant_id;

END;
$$ LANGUAGE plpgsql;

SELECT * FROM view_tenant_information(1);

-- inner query analysis:
EXPLAIN ANALYZE SELECT first_name, last_name, email, phone
FROM tenants 
WHERE tenant_id = 1;
-- cost: 8.29 (already with an index on primary key tenant_id of tenants)

---------------------------------------------------------------------
-- ******
-- 32: view tenants whose contracts with the landlord are still active
-- 
CREATE OR REPLACE FUNCTION view_active_tenants(input_landlord_id INT)
RETURNS SETOF INT AS $$
BEGIN

    RETURN QUERY
    SELECT tenant_id 
    FROM requests R
    JOIN contracts C ON C.request_id = R.request_id
    JOIN apartments A ON R.apartment_id = A.apartment_id
    WHERE A.landlord_id = input_landlord_id
        AND C.start_date <= CURRENT_DATE
        AND C.end_date >= CURRENT_DATE;

END;
$$ LANGUAGE plpgsql;

SELECT * FROM view_active_tenants(1);

-- inner query analysis
EXPLAIN SELECT tenant_id 
FROM requests R
JOIN contracts C ON C.request_id = R.request_id
JOIN apartments A ON R.apartment_id = A.apartment_id
WHERE A.landlord_id = 5
    AND C.start_date <= CURRENT_DATE
    AND C.end_date >= CURRENT_DATE;
--> total cost: 376.03

-- index on contracts.end_date
CREATE INDEX idx_contracts_end_date ON contracts(end_date);

--> cost: 327.79

-- additional index on contracts.request_id
CREATE INDEX idx_contracts_request_id ON contracts(request_id);
--> cost: 257.62

-- additional index on apartments.landlord_id
CREATE INDEX idx_apartments_landlord_id ON apartments(landlord_id);
--> cost: 202.71

-- additional index on requests.apartment_id
CREATE INDEX idx_requests_apartment_id ON requests(apartment_id);
--> cost: 60.09
-- since requests table is large, using index on apartment_id proves to be vital

DROP INDEX idx_contracts_end_date;
DROP INDEX idx_contracts_request_id;
DROP INDEX idx_apartments_landlord_id;
DROP INDEX idx_requests_apartment_id;


---------------------------------------------------------------------
-- 34: view tenants who have not paid the bill for a specific month
--
CREATE OR REPLACE FUNCTION view_indebt_tenants(input_landlord_id INT, input_month CHAR(7))
RETURNS SETOF INT AS $$
BEGIN 

    RETURN QUERY
    SELECT R.tenant_id
    FROM apartments A
    JOIN requests R ON R.apartment_id = A.apartment_id
    JOIN contracts C ON C.request_id = R.request_id
    JOIN bills B ON B.contract_id = C.contract_id
    LEFT JOIN payment_details PD ON PD.bill_id = B.bill_id
    WHERE A.landlord_id = input_landlord_id
        AND B.month = input_month
        AND PD.bill_id IS NULL;

END;
$$ LANGUAGE plpgsql;

SELECT view_indebt_tenants(773, '2024-12');

-- inner query analysis
EXPLAIN ANALYZE SELECT R.tenant_id
FROM apartments A
JOIN requests R ON R.apartment_id = A.apartment_id
JOIN contracts C ON C.request_id = R.request_id
JOIN bills B ON B.contract_id = C.contract_id
LEFT JOIN payment_details PD ON PD.bill_id = B.bill_id
WHERE A.landlord_id = 773
    AND B.month = '2024-12'
    AND PD.bill_id IS NULL;
-- cost: 355.98
-- (already with composite B-tree index on (contract_id, month) of bills table)
-- (already composite B-tree index on (tenant_id, bill_id) of payment_details table)

-- with index on apartments.landlord_id
CREATE INDEX idx_apartments_landlord_id ON apartments(landlord_id);
-- cost: 311.78

-- with additional index on requests.apartment_id;
CREATE INDEX idx_requests_apartment_id ON requests(apartment_id);
-- cost: 237.59

-- with additional index on contracts.request_id
CREATE INDEX idx_contracts_request_id ON contracts(request_id);
-- cost: 155.83

DROP INDEX idx_apartments_landlord_id;
DROP INDEX idx_requests_apartment_id;
DROP INDEX idx_contracts_request_id;

---------------------------------------------------------------------
-- *****
-- 37: retrieve total number of requests during a specific time of whole system
--
CREATE OR REPLACE FUNCTION get_total_requests(input_date1 DATE, input_date2 DATE)
RETURNS INT AS $$
DECLARE 
    v_total_requests INT;
BEGIN

    -- check if user misinput date1 and date
    IF input_date1 > input_date2 THEN
        RAISE EXCEPTION 'Invalid date range, date1 must be smaller than date2';
    END IF;

    SELECT COUNT(*) 
    INTO v_total_requests
    FROM requests 
    WHERE request_date >= input_date1
        AND request_date <= input_date2;

    RETURN v_total_requests;

END;
$$ LANGUAGE plpgsql;

SELECT get_total_requests('2022-01-01', '2022-12-31');

-- inner query analysis
EXPLAIN ANALYZE SELECT COUNT(*) 
FROM requests 
WHERE request_date >= '2022-01-01'
    AND request_date <= '2022-12-31';
--> cost without index: 213.13

-- with index of request_date
CREATE INDEX idx_requests_request_date ON requests (request_date);
--> cost for whole operation: 61.81

DROP INDEX idx_requests_request_date;

---------------------------------------------------------------------

-- 38: get top 10 apartments with highest rating
--
SELECT apartment_id, ROUND(SUM(score) / CAST(COUNT(*) AS DECIMAL), 2) AS avg_score, COUNT(*) AS num
FROM rating 
GROUP BY apartment_id
ORDER BY avg_score DESC
LIMIT 10;

-------------------------------------------------------------------
-------------- Tran Viet Anh --------------------------------------
-------------------------------------------------------------------
-- pg_restore -U postgres -d apartments_project 'D:\Database Lab\apartments_management_system\database_backup'
-------------------------------------------------------------------1
-- OK
create or replace function request_to_rent_apartment (IN tenantID integer, apartmentID integer, startMonth char(7), rentDuration integer) returns void
as
$$
declare
    newID integer;
    rentEndDate date;
    rentDate date;
begin
    select max(request_id) + 1 into newID from requests;
    insert into requests (request_id, tenant_id, apartment_id, request_date, start_month, duration)
    values (newID, tenantID, apartmentID, current_date, startMonth, rentDuration);

    raise notice 'Request %: Tenant % requests for apartment % for % months.', newID, tenantID, apartmentID, rentDuration;

end;
$$
language plpgsql
volatile;


select apartment_id, start_date, end_date, contract_id
from contracts c
join requests r on c.request_id = r.request_id
where apartment_id = 600;

select apartment_id, start_date, end_date, contract_id
from contracts c
join requests r on c.request_id = r.request_id
where tenant_id = 500;

select tenant_id, apartment_id, start_date, end_date
from contracts c
join requests r on c.request_id = r.request_id
where tenant_id = 580;

select request_to_rent_apartment(580, 600, '2025-06', 3);
select request_to_rent_apartment(500, 600, '2025-01', 8);


-- OK
-------------------------------3
-- view unpaid bill - 13
create or replace function pay_monthly_bills (IN billID integer, IN tenantID integer) returns void
as
$$
begin
    if (extract(day from current_date) < 5) then
        return;
    end if;

    if (exists (
        select 1
        from payment_details
        where bill_id = billID
    )) then
        return;
    end if;

    insert into payment_details (bill_id, tenant_id, payment_date)
    values (billID, tenantID, current_date);

    raise notice '%s: Bill %d successfully.', current_date, billID;
end;
$$
language plpgsql
volatile;


select bill_id, tenant_id, c.contract_id, start_date, end_date
from bills b left join contracts c on b.contract_id = c.contract_id
left join requests r on c.request_id = r.request_id
where bill_id not in (
    select bill_id from payment_details
);

select pay_monthly_bills(18, 203);

-- CANOOT TEST
-------------------------------4
-- view rental history - 10 (Hao)

create or replace function cancel_contract (IN tenantID integer, IN contractID integer) returns void
as
$$
declare
    startDate date;
begin
    select start_date into startDate
    from contracts
    where contract_id = contractID;

    if ((extract(year from current_date) - extract(year from startDate)) * 12
        + extract(month from current_date) - extract(month from startDate) < 3) then
        return;
    end if;

    if (extract(day from current_date) < 5) then
        return;
    end if;

    if (not exists (
        select 1
        from payment_details p join bills b on (b.bill_id = p.bill_id)
        where b.contract_id = contractID
    )) then
        raise notice 'You haven''t paid bill. Cannot cancel contract.';
        return;
    end if;

    update contracts
    set end_date = current_date
    where contract_id = contractID;

end;
$$
language plpgsql
volatile;


-- view unpaid bill - 13
select bill_id, tenant_id, c.contract_id, start_date, end_date
from bills b left join contracts c on b.contract_id = c.contract_id
left join requests r on c.request_id = r.request_id
where bill_id not in (
    select bill_id from payment_details
);

select pay_monthly_bills(705, 28);

select cancel_contract(705,4);

-- OK
-------------------------------5
DROP FUNCTION filter_apartment(integer,integer,integer,integer,character varying,character varying,character varying);

create or replace function filter_apartment (IN startSize integer, IN endSize integer, IN numBedrooms integer, IN numBathrooms integer,
IN yn_Kitchen varchar, IN yn_Air_conditioner varchar, IN yn_TV varchar) returns setof apartments
as
$$
begin
    return query
    select apartment_id, address, size, bedrooms, bathrooms, kitchen, air_conditioner, tv, landlord_id
    from apartments
    where size >= coalesce(startSize,0)
      and size <= coalesce(endSize,0)
      and bedrooms = coalesce(numBedrooms, bedrooms)
      and bathrooms = coalesce(numBathrooms, bathrooms)
      and kitchen = coalesce(yn_Kitchen, kitchen)
      and air_conditioner = coalesce(yn_Air_conditioner, air_conditioner)
      and tv = coalesce(yn_TV, tv);

end;
$$
language plpgsql
stable;

select *
from apartments
where kitchen = 'YES' and air_conditioner = 'YES' and tv = 'YES';

select filter_apartment(100,120,null,null,'YES','YES','YES');

explain analyze 
select * from filter_apartment(100,120,null,null,'YES','YES','YES');

EXPLAIN ANALYZE
SELECT apartment_id, address, size, bedrooms, bathrooms, kitchen, air_conditioner, tv, landlord_id
FROM apartments
WHERE size >= COALESCE(100, 0)
  AND size <= COALESCE(120, 0)
  AND bedrooms = COALESCE(NULL, bedrooms)
  AND bathrooms = COALESCE(NULL, bathrooms)
  AND kitchen = COALESCE('YES', kitchen)
  AND air_conditioner = COALESCE('YES', air_conditioner)
  AND tv = COALESCE('YES', tv);

-- OK
-------------------------------7
-- view all pending requests - 11
create or replace function cancel_unwanted_requests (IN requestID integer) returns void
as
$$
begin
    delete from requests
    where request_id = requestID;

end;
$$
language plpgsql
volatile;


-- OK
-------------------------------9
create or replace function rate_apartment (IN tenantID integer, IN apartmentID integer, IN Score integer) returns void
as
$$
begin
    insert into rating (tenant_id, apartment_id, score)
    values (tenantID, apartmentID, Score);

end;
$$
language plpgsql
volatile;


select * from rating;

select tenant_id, apartment_id
from requests r left join contracts c using (request_id)
where end_date < current_date
    and not exists (
        select 1
        from rating where tenant_id = r.tenant_id and apartment_id = r.apartment_id
    );

insert into requests (request_id, tenant_id, apartment_id, request_date, start_month, duration)
values (49994,200,300,'2025-01-01','2025-02',4);
insert into contracts (contract_id, start_date, end_date, rent_amount, request_id)
values (4406,'2025-02-01', '2025-05-31',1000,49994);

select rate_apartment(200,300,10);



-- OK
-------------------------------19
create or replace function view_total_money_on_apartment (IN tenantID integer, IN apartmentID integer) returns void
as
$$
declare
    totalMoney integer;
begin
    with tmp as (
        select bill_id, price
        from requests r
        left join contracts c on r.request_id = c.request_id
        left join bills b on c.contract_id = b.contract_id
        where r.tenant_id = tenantID and r.apartment_id = apartmentID
    )
    select coalesce(sum(price),0) into totalMoney
    from tmp
    where bill_id in (
        select bill_id from payment_details where tenant_id = tenantID
    );

    raise notice 'Total money you paid for apartment %d is: %d', apartmentID, totalMoney;
end;
$$
language plpgsql
stable;


select *
from payment_details p join bills b on p.bill_id = b.bill_id
where tenant_id = 600;

select * from contracts where contract_id = 3218;
select * from requests where request_id = 42490;

select view_total_money_on_apartment(600,516);

explain analyze 
select * from view_total_money_on_apartment(600,516);


-- OK
-------------------------------23
create or replace function view_monthly_expected_earning (IN landlordID integer, IN inputMonth char(7)) returns void
as
$$
declare
    totalMoney integer;
begin
    with tmp as (
        select contract_id
        from contracts c
        join requests r on c.request_id = r.request_id
        join apartments a on r.apartment_id = a.apartment_id
        where a.landlord_id = landlordID
    )
    select coalesce(sum(price),0) into totalMoney
    from tmp t join bills b on t.contract_id = b.contract_id
    where b.month = inputMonth;

    raise notice 'Total expected earnings for landlord % is: %d', landlordID, totalMoney;

end;
$$
language plpgsql
stable;

select contract_id, request_id, apartment_id, landlord_id, rent_amount, start_date, end_date
from contracts
join requests using (request_id) 
join apartments using (apartment_id)
where landlord_id = 600;

select view_monthly_expected_earning(600,'2022-10');

explain analyze select * from view_monthly_expected_earning(600,'2022-10');

-- OK
-------------------------------24
create or replace function view_monthly_received_earning (IN landlordID integer, IN inputMonth char(7)) returns void
as
$$
declare
    totalMoney integer;
begin
    with tmp as (
        select contract_id
        from contracts c
        join requests r on c.request_id = r.request_id
        join apartments a on r.apartment_id = a.apartment_id
        where a.landlord_id = landlordID
    )
    select coalesce(sum(price),0) into totalMoney
    from tmp t join bills b on t.contract_id = b.contract_id
    where b.month = inputMonth
    and exists (
        select 1
        from payment_details p
        where b.bill_id = p.bill_id
    );

    raise notice 'Total received earnings for landlord % is: %d', landlordID, totalMoney;

end;
$$
language plpgsql
stable;


select contract_id, request_id, apartment_id, landlord_id, rent_amount, start_date, end_date
from contracts
join requests using (request_id) 
join apartments using (apartment_id)
where landlord_id = 500;

    with tmp as (
            select contract_id
            from contracts c
            join requests r on c.request_id = r.request_id
            join apartments a on r.apartment_id = a.apartment_id
            where a.landlord_id = 500
        )
        select t.contract_id, bill_id
        from tmp t join bills b on t.contract_id = b.contract_id
        where b.month = '2022-11'
        and exists (
            select 1
            from payment_details p
            where b.bill_id = p.bill_id
        );

select view_monthly_received_earning(500,'2022-11');

explain analyze select * from view_monthly_received_earning(500,'2022-11');

-- OK
-------------------------------27
create or replace function view_blacklist_tenants (IN landlordID integer) returns void
as
$$
declare
    v_loop1 record;
    v_loop2 record;
    payDay integer;
    lateTime integer;
    billDate date;
begin
    for v_loop1 in (
        select tenant_id, contract_id
        from contracts c 
        join requests r on c.request_id = r.request_id
        join apartments a on r.apartment_id = a.apartment_id
        where landlord_id = landlordID
    ) loop
        lateTime := 0;

        for v_loop2 in (
            select p.bill_id, payment_date, month
            from bills b left join payment_details p on b.bill_id = p.bill_id
            where b.contract_id = v_loop1.contract_id and p.tenant_id = v_loop1.tenant_id
        ) loop 
            payDay := 0;
            billDate := to_date(v_loop2.month || '-05', 'YYYY-MM-DD');
            payDay := (extract(year from v_loop2.payment_date) - extract(year from billDate)) * 365
                    + (extract(month from v_loop2.payment_date) - extract(month from billDate)) * 30
                    + (extract(day from v_loop2.payment_date) - extract(day from billDate));

            if (v_loop2.payment_date is null) then
                lateTime := lateTime + 1;
            end if;

            if (payDay >= 6) then
                lateTime := lateTime + 1;
            end if;
        end loop;

        if (lateTime >= 1) then
            raise notice 'Landlord %: Tenant %: % times', landlordID, v_loop1.tenant_id, lateTime;
        end if;
    end loop;

end;
$$
language plpgsql
stable;

create or replace function test() returns void
as
$$
declare
    id integer;
begin
    for id in (
        select landlord_id from landlords
    ) loop
        perform view_blacklist_tenants(id);
    end loop;
end;
$$
language plpgsql;

select test();

select view_blacklist_tenants(1000);

explain analyze 
select * from view_blacklist_tenants(1000);

landlord: 1000
tenant 992: 16 times
tenant 958: 16 times
tenant 723: 4 times

select contract_id, tenant_id, landlord_id
from contracts
join requests using (request_id)
join apartments using (apartment_id)
where landlord_id = 1000;

select * from bills where contract_id = 8341;
select * from payment_details where bill_id = 44650;
select * from payment_details where bill_id = 44651;
select * from payment_details where bill_id = 44652;
select * from payment_details where bill_id = 44653;


-- OK
---------------------------------17
create or replace function view_unaccepted_request (IN tenantID integer) 
returns table(requestID integer, tenatnID integer, apartmentID integer, requestDate date, startMonth char(7), Dura_tion integer)
as
$$
begin
    return query
    select *
    from requests
    where tenant_id = tenantID
    and request_id not in (
        select request_id from contracts
    );

end;
$$
language plpgsql
stable;

select * from view_unaccepted_request(500);

EXPLAIN ANALYZE
SELECT *
FROM requests
WHERE tenant_id = 500
  AND request_id NOT IN (
      SELECT request_id FROM contracts
  );
 

-- OK
---------------------------------20
create or replace function accept_request (IN requestID integer, IN rentAmount integer) returns void
as
$$
declare
    newID integer;
    startMonth char(7);
begin
    select start_month into startMonth from requests where request_id = requestID;

    if (to_date(startMonth || '-01', 'YYYY-MM-DD') <= current_date) then
        raise notice 'Request period has exceeded';
        return;
    end if;

    select max(contract_id) + 1 into newID
    from contracts;

    insert into contracts (contract_id, rent_amount, request_id)
    values (newID, rentAmount, requestID);

    raise notice 'Request %s has been accpeted with rent amount = %', requestID, rentAmount;

end;
$$
language plpgsql
volatile;

select * from view_unaccepted_request(500);

select * from accept_request(631,1000);

explain analyze select * from accept_request(631,1000);


-- OK
---------------------------------25
create or replace function terminate_contract (IN contractID integer) returns void
as
$$
declare
    v_loop record;
    dateDifference integer;
    dateCheck integer;
begin
    dateCheck := 0;
    for v_loop in (
        select b.bill_id, b.month, p.payment_date 
        from contracts c left join bills b on c.contract_id = b.contract_id
        left join payment_details p on b.bill_id = p.bill_id
        where c.contract_id = contractID
    ) loop
        if (v_loop.payment_date is not null and v_loop.payment_date > to_date(v_loop.month || '-20', 'YYYY-MM-DD')) then
            dateCheck := 1;
            exit;
        elsif ((current_date > to_date(v_loop.month || '-20', 'YYYY-MM-DD') and v_loop.payment_date is null)) then
            dateCheck := 1;
            exit;
        end if;
    end loop;

    if (dateCheck = 0) then
        raise notice 'Tenant has paid the bill. Landlord cannot terminate contract.';
        return;
    end if;

    delete from contracts where contract_id = contractID;
    raise notice 'Contract %d has been terminated.', contractID;

end;
$$
language plpgsql
volatile;

select *
from contracts c left join bills b on c.contract_id = b.contract_id
left join payment_details p on b.bill_id = p.bill_id
where p.bill_id is null;

select *
from contracts c left join bills b on c.contract_id = b.contract_id
left join payment_details p on b.bill_id = p.bill_id
where c.contract_id = 400;

select * from terminate_contract(400);


-- OK
---------------------------------26
create or replace function view_payment_status (IN contractID integer)
returns table(contract integer, billID integer, bill_month char(7), paymentDate date)
as
$$
begin
    return query
    select c.contract_id, b.bill_id, b.month, p.payment_date
    from contracts c join bills b on c.contract_id = b.contract_id
    left join payment_details p on b.bill_id = p.bill_id
    where c.contract_id = contractID;

end;
$$
language plpgsql
stable;

select c.contract_id, b.bill_id, b.month, p.payment_date
    from contracts c join bills b on c.contract_id = b.contract_id
    left join payment_details p on b.bill_id = p.bill_id
    where p.bill_id is null;

select * from view_payment_status(4);

explain analyze
select c.contract_id, b.bill_id, b.month, p.payment_date
    from contracts c join bills b on c.contract_id = b.contract_id
    left join payment_details p on b.bill_id = p.bill_id
    where c.contract_id = 4;


-- 
---------------------------------35
create or replace function add_apartment(IN aress varchar, IN sze integer, IN berooms integer, IN barooms integer, IN kchen varchar, IN aconditioner varchar, IN t_v varchar, IN landlordID integer)
returns void
as
$$  
declare
    newApartmentID integer;
begin
    if exists (
        select 1 from apartments where address = aress
    ) then
        raise notice 'Address % is already existed.', aress;
        return;
    end if;

    select max(apartment_id) + 1 into newApartmentID
    from apartments;

    insert into apartments (apartment_id, address, size, bedrooms, bathrooms, kitchen, air_conditioner, tv, landlord_id)
    values (newApartmentID, sze, berooms, barooms, kchen, aconditioner, t_v, landlordID);

    raise notice 'Apartment with address % has been inserted.', add_ress;

end;
$$
language plpgsql
volatile;

select * from add_apartment('Hanoi001', 100, 2, 2, 'YES', 'YES', 'YES', 'YES', 100);


-------------------------------------------------------------------
-------------- Vu Nguyen Hao --------------------------------------
-------------------------------------------------------------------
--TRUNCATE rating, bills, payment_details;
--COPY rating FROM 'D:\\Documents\\HUST\\Database Lab\\Project\\apartments_management_system\\data\\rating\\rating.csv' DELIMITER ',' CSV HEADER;
--COPY bills FROM 'D:\\Documents\\HUST\\Database Lab\\Project\\apartments_management_system\\data\\bills\\bills.csv' DELIMITER ',' CSV HEADER;
--COPY payment_details FROM 'D:\\Documents\\HUST\\Database Lab\\Project\\apartments_management_system\\data\\payment_details\\payment_details.csv' DELIMITER ',' CSV HEADER;

--Query 10--
DROP FUNCTION IF EXISTS view_pending_requests, view_apartment_info, view_landlord_info, view_recived_requests,view_request_statistics, view_tenant_accept_rate;
DROP MATERIALIZED VIEW IF EXISTS apartment_avg_rating;

CREATE INDEX idx_apartments_apartment_id ON apartments(apartment_id);
CREATE INDEX idx_requests_apartment_id ON requests(apartment_id);
CREATE INDEX idx_requests_tenant_id ON requests(tenant_id);
CREATE INDEX idx_requests_request_id ON requests(request_id);
CREATE INDEX idx_bills_bill_id ON bills(bill_id);

CREATE OR REPLACE FUNCTION view_rental_history(p_tenant_id INT)
RETURNS TABLE (apartment_id INT, contract_id INT, start_date DATE, end_date DATE)
AS $$
BEGIN
    RETURN QUERY 
        SELECT a.apartment_id, c.contract_id, c.start_date, c.end_date
        FROM contracts c
        INNER JOIN requests r ON c.request_id = r.request_id
        INNER JOIN apartments a ON r.apartment_id = a.apartment_id
        WHERE r.tenant_id = p_tenant_id;
END;
$$ LANGUAGE plpgsql;
-- select * from view_rental_history(666);

--Query 11: view all requests of tenant that have not been accepted--
CREATE OR REPLACE FUNCTION view_pending_requests(p_tenant_id INT)
RETURNS TABLE (request_id INT, apartment_id INT, start_month CHAR(7), request_date DATE, duration INT)
AS $$
BEGIN
    RETURN QUERY
        SELECT r.request_id, r.apartment_id, r.start_month, r.request_date, r.duration
        FROM requests r 
        LEFT JOIN contracts c ON r.request_id = c.request_id
        WHERE r.tenant_id = p_tenant_id AND c.contract_id IS NULL;
END;
$$ LANGUAGE plpgsql;


--Query 13: view unpaid bills--
CREATE OR REPLACE FUNCTION view_unpaid_bills(p_tenant_id INT)
RETURNS TABLE (
    bill_id INT,
    contract_id INT,
    month CHAR(7),
    price INT
)
AS $$
BEGIN 
    RETURN QUERY
        SELECT b.* 
        FROM bills b
        JOIN contracts c ON b.contract_id = c.contract_id
        JOIN requests r ON c.request_id = r.request_id
        WHERE r.tenant_id = p_tenant_id AND b.bill_id NOT IN (
            SELECT p.bill_id
            FROM payment_details p
        );
END;
$$ LANGUAGE plpgsql;

--Query 14: show landlord info and avg rating--
---create materialized view since this rating can be call multiple time
CREATE MATERIALIZED VIEW apartment_avg_rating AS
    SELECT apartment_id, AVG(score)
    FROM rating
    GROUP BY apartment_id;

---create index for faster query
CREATE INDEX idx_mview_apartmentavgrating_apartment_id ON apartment_avg_rating(apartment_id);

---Query 36: view apt rating---
CREATE OR REPLACE FUNCTION view_apartment_rating(p_apt_id INT)
RETURNS DECIMAL(2,1)
AS $$
DECLARE
    v_avg_score DECIMAL(2,1);
BEGIN 
    --update the view each time you call the function
    REFRESH MATERIALIZED VIEW apartment_avg_rating;
    --retrieve the data in the cache
    SELECT avg
    INTO v_avg_score
    FROM apartment_avg_rating
    WHERE apartment_id = p_apt_id;
    --return the score
    RETURN v_avg_score;
END;
$$ LANGUAGE plpgsql;

---CALCULATE AVG RATING OF ALL APARTMENT OF A LANLORD---
CREATE OR REPLACE FUNCTION cal_avg_all_rating(p_landlord_id INT)
RETURNS DECIMAL(2,1)
AS $$
DECLARE
    v_avg_score DECIMAL(2,1);
BEGIN 
    SELECT AVG(apt_avg.apt_rating)
    INTO v_avg_score
    FROM (
        --call the function to calculate each apt rating
        SELECT view_apartment_rating(a.apartment_id) AS apt_rating
        FROM apartments a
        WHERE a.landlord_id = p_landlord_id --p_landlord_id
    ) apt_avg;

    RETURN v_avg_score;
END;
$$ LANGUAGE plpgsql;
--SELECT *, view_apartment_rating(apartment_id) AS rating FROM apartments
--WHERE landlord_id = 409;
--SELECT cal_avg_all_rating(409);

---VIEW LANDLORD INFO---
CREATE OR REPLACE FUNCTION view_landlord_info(p_landlord_id INT)
RETURNS TABLE (
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    phone VARCHAR(50),
    average_apartments_rating DECIMAL(2,1)
)
AS $$
BEGIN 
    RETURN QUERY
        SELECT l.first_name, l.last_name, l.email, l.phone, cal_avg_all_rating(l.landlord_id)
        FROM landlords l
        WHERE l.landlord_id = p_landlord_id;
END;
$$ LANGUAGE plpgsql;
--SELECT * FROM view_landlord_info(409);

--Query 12: view information of a specific apartment
CREATE OR REPLACE FUNCTION view_apartment_info(p_apartment_id INT)
RETURNS TABLE (
    address VARCHAR(50),
    size INT,
    bedrooms INT,
    bathrooms INT,
    kitchen VARCHAR(3),
    air_conditioner VARCHAR(3),
    tv VARCHAR(3),
    rating_score DECIMAL(2,1))
AS $$
BEGIN 
    SELECT address, size, bedrooms, bathrooms, kitchen, air_conditioner, tv, view_apartment_rating(apartment_id)
    FROM apartments
    WHERE apartment_id = p_apartment_id;
END;
$$ LANGUAGE plpgsql;

--Query 15: view apt rented time--
CREATE OR REPLACE FUNCTION view_apartment_rented_times(p_apartment_id INT)
RETURNS INT
AS $$
DECLARE
    v_count INT;
BEGIN 
    SELECT COUNT(c.contract_id)
    INTO v_count
    FROM contracts c
    JOIN requests r ON c.request_id = r.request_id
    WHERE r.apartment_id = p_apartment_id;

    RETURN v_count;
END;
$$ LANGUAGE plpgsql;
--select view_apartment_rented_times(10);

--Query 18: view total money paid of a tenant--
CREATE OR REPLACE FUNCTION view_total_money_spent(p_tenant_id INT)
RETURNS INT 
AS $$
DECLARE
    v_total_amount INT;
BEGIN 
    SELECT SUM(b.price)
    INTO v_total_amount
    FROM payment_details p
    JOIN bills b ON p.bill_id = b.bill_id
    WHERE p.tenant_id = p_tenant_id;

    RETURN v_total_amount;
END;
$$ LANGUAGE plpgsql;
--SELECT view_total_money_spent(409); 

--Query 18.5: total money spent on a month
CREATE OR REPLACE FUNCTION total_money_this_month(p_tenant_id INT, p_month INT)
RETURNS INT
AS $$
DECLARE
    v_total_amount INT;
BEGIN 
SELECT 
    SUM(b.price)
    INTO v_total_amount
    FROM payment_details p
    JOIN bills b ON p.bill_id = b.bill_id
    WHERE p.tenant_id = p_tenant_id AND EXTRACT(MONTH FROM TO_DATE(b.month || '-01', 'YYYY-MM-DD')) = p_month;

    RETURN v_total_amount;
END;
$$ LANGUAGE plpgsql;

--Query 22: view idle apartment in the next *input* month
CREATE OR REPLACE FUNCTION view_idle_apartment_future(p_landlord_id INT, p_month INT)
RETURNS TABLE (apartment_id INT)
AS $$
BEGIN 
    RETURN QUERY
        SELECT a.apartment_id
        FROM apartments a
        LEFT JOIN (
            SELECT 
                r.apartment_id,
                MIN(c.start_date) AS next_contract_start, --nearest contract start date
                MAX(c.end_date) AS last_contract_end -- last contract end date
            FROM requests r
            LEFT JOIN contracts c ON r.request_id = c.request_id
            GROUP BY r.apartment_id
            ) contract_summary ON a.apartment_id = contract_summary.apartment_id
        WHERE a.landlord_id = p_landlord_id AND (
            contract_summary.apartment_id IS NULL -- No contracts exist for this apartment
            OR contract_summary.last_contract_end < DATE_TRUNC('month', CURRENT_DATE) -- Last contract ends before this month
            OR contract_summary.next_contract_start > DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' * (p_month + 1) -- Next contract starts after the specified period
        );
END;
$$ LANGUAGE plpgsql;
--select view_idle_apartment_future(409,2);

--Query 28: fine tenants 20% for late bill payment--
CREATE OR REPLACE FUNCTION fine_late_payment(p_bill_id INT)
RETURNS VOID
AS $$
DECLARE
    v_bill_date DATE;
    v_bill_price INT;
    v_temp INT;
BEGIN 
    --Pass value to param
    SELECT TO_DATE(b.month || '-01', 'YYYY-MM-DD'), b.price
    INTO v_bill_date, v_bill_price
    FROM bills b
    WHERE b.bill_id = p_bill_id;
    
    --check if the bill is paid?
    IF EXISTS (
            SELECT 1
            FROM payment_details p
            WHERE p.bill_id = p_bill_id
        ) THEN
            RAISE EXCEPTION 'The bill has already been paid!';
    ELSE 
        --in case havent pass the 10th day of the month
        IF (CURRENT_DATE - v_bill_date) <10 THEN 
            RAISE EXCEPTION 'The payment is not overdue!';
        ELSE 
            --process to fine the tenant by 20%
            v_temp := v_bill_price;
            v_bill_price := v_bill_price * 120 / 100;
            UPDATE bills
            SET price = v_bill_price
            WHERE bill_id = p_bill_id;
            
            RAISE NOTICE 'Tenant has been fined 20%% of the bill (%)', v_bill_price;
            --return the old value to continue query (delete this and the temp param later)
            UPDATE bills
            SET price = v_temp
            WHERE bill_id = p_bill_id;
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;
--SELECT fine_late_payment(2);

--Query 29: view all recived requests for an apartment--
---Function to check request status
CREATE OR REPLACE FUNCTION check_request_status(p_request_id INT)
RETURNS VARCHAR(8)
AS $$
DECLARE
    v_request_status VARCHAR(8);
    v_start_date DATE;
BEGIN
    SELECT TO_DATE(r.start_month || '-01', 'YYYY-MM-DD')
    INTO v_start_date
    FROM requests r
    WHERE r.request_id = p_request_id;
    
    --check codition
    IF EXISTS (
        SELECT 1 
        FROM contracts
        WHERE request_id = p_request_id
    ) THEN 
        v_request_status := 'APPROVED';
    ELSE 
        IF (CURRENT_DATE - v_start_date) > 0 THEN
            v_request_status := 'OVERDUED';
        ELSE 
            v_request_status := 'PENDING';
        END IF;
    END IF;

    RETURN v_request_status;
END;
$$ LANGUAGE plpgsql;
--FUNCTION ABOVE MIGHT CHANGE LATER

CREATE OR REPLACE FUNCTION view_received_requests(p_apartment_id INT)
RETURNS TABLE (
    request_id INT,
    start_month CHAR(7),
    request_date DATE,
    duration INT,
    status VARCHAR(8)
)
AS $$
BEGIN 
    RETURN QUERY
        SELECT r.request_id, r.start_month, r.request_date, r.duration, check_request_status(r.request_id)
        FROM requests r
        WHERE r.apartment_id = p_apartment_id;
END;
$$ LANGUAGE plpgsql;
--select * from view_received_requests(904);

--Query 30: view all apartment with time they requested--
---OPTIMIZED---
CREATE OR REPLACE FUNCTION view_request_statistics(p_landlord_id INT)
RETURNS TABLE (
    apartment_id INT,
    number_of_requests BIGINT
)
AS $$
BEGIN 
    RETURN QUERY
        SELECT r.apartment_id, COUNT(r.request_id)
        FROM requests r
        JOIN apartments a ON r.apartment_id = a.apartment_id
        WHERE a.landlord_id = p_landlord_id
        --WHERE EXISTS (
            --SELECT 1 
            --FROM apartments a
            --WHERE a.apartment_id = r.apartment_id AND a.landlord_id = p_landlord_id
        --)
        
        GROUP BY r.apartment_id;
END;
$$ LANGUAGE plpgsql;
select * from view_request_statistics(409);

--Query 33: view return rate of an apartment--
---OPTIMIZED---
CREATE OR REPLACE FUNCTION view_apartment_return_rate(p_apartment_id INT)
RETURNS NUMERIC 
AS $$
DECLARE
    v_return_rate NUMERIC;
BEGIN
    -- Calculate return rate
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 0 -- No tenants rented the apartment
            ELSE (
                COUNT(DISTINCT CASE WHEN sub.contract_count > 1 THEN sub.tenant_id END)::NUMERIC / 
                COUNT(DISTINCT sub.tenant_id) * 100
            )
        END
    INTO v_return_rate
    FROM (
        SELECT r.tenant_id, COUNT(c.contract_id) AS contract_count
        FROM contracts c
        JOIN requests r ON c.request_id = r.request_id
        WHERE r.apartment_id = p_apartment_id
        GROUP BY r.tenant_id
    ) sub;

    RETURN v_return_rate;
END;
$$ LANGUAGE plpgsql;
--select view_apartment_return_rate(1477);
--SELECT r.apartment_id, COUNT(c.contract_id) AS rental_count
--FROM contracts c
--JOIN requests r ON c.request_id = r.request_id
--GROUP BY r.apartment_id
--HAVING COUNT(c.contract_id) > 2;
--select * from requests
--where apartment_id = 1477;

--Query 33.5: view return rate of a landlord--
CREATE OR REPLACE FUNCTION view_landlord_return_rate(p_landlord_id INT)
RETURNS NUMERIC
AS $$
DECLARE
    v_return_rate NUMERIC;
    v_count_returning_tenants BIGINT;
    v_count_total_tenants BIGINT;
BEGIN 
    --calculate number of tenants who rented more than once
    SELECT COUNT(*)
    INTO v_count_returning_tenants
    FROM (
        SELECT r.tenant_id
        FROM contracts c
        JOIN requests r ON c.request_id = r.request_id
        WHERE EXISTS( --only check bool in apartments table without joining
            SELECT 1
            FROM apartments a
            WHERE a.apartment_id = r.apartment_id AND a.landlord_id = p_landlord_id
        )
        GROUP BY r.tenant_id
        HAVING COUNT(c.contract_id) > 1
    );

    --calculate total number of rented times
    SELECT COUNT(*)
    INTO v_count_total_tenants
    FROM contracts c
    JOIN requests r ON c.request_id = r.request_id
    WHERE EXISTS(
            SELECT 1
            FROM apartments a
            WHERE a.apartment_id = r.apartment_id AND a.landlord_id = p_landlord_id
    );

    --if the apt hasnt been rented once -> rr = 0
    IF v_count_total_tenants = 0 THEN
        RETURN 0;
    END IF;

    --calculate returning rate
    v_return_rate := (v_count_returning_tenants::NUMERIC / v_count_total_tenants) * 100;
    RETURN v_return_rate;
END;
$$ LANGUAGE plpgsql;
--SELECT view_landlord_return_rate(409);

--Query 35: view request accept rate--
---OPTIMIZED---
CREATE OR REPLACE FUNCTION view_tenant_accept_rate(p_tenant_id INT)
RETURNS NUMERIC
AS $$
DECLARE
    v_accept_rate DECIMAL(5,2);
BEGIN
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 0 -- Handle division by zero
            ELSE (COUNT(c.contract_id)::NUMERIC / COUNT(*) * 100)
        END
    INTO v_accept_rate
    FROM requests r
    LEFT JOIN contracts c ON r.request_id = c.request_id
    WHERE r.tenant_id = p_tenant_id;

    RETURN v_accept_rate;
END;
$$ LANGUAGE plpgsql;
--select view_tenant_accept_rate(666);

