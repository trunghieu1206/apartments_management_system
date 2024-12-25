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
ADD CONSTRAINT requests_check_start_month_after_request_date \i 'D:\\Documents\\HUST\\Database Lab\\Project\\apartments_management_system\\data\\requests\\requests.sql'
CHECK (TO_DATE(start_month || '-01', 'YYYY-MM-DD') > request_date);

-- start_month must be within 1 year of request_date
ALTER TABLE requests
ADD CONSTRAINT check_start_month_within_1_year
CHECK (TO_DATE(start_month || '-01', 'YYYY-MM-DD') <= request_date + INTERVAL '1 year');

-- `contracts` table
ALTER TABLE contracts 
ADD CONSTRAINT contracts_fk_requests FOREIGN KEY (request_id)
REFERENCES requests(request_id)
ON UPDATE CASCADE -- 
ON DELETE SET NULL; 

-- `bills` table
ALTER TABLE bills
ADD CONSTRAINT bills_fk_contracts FOREIGN KEY (contract_id)
REFERENCES contracts(contract_id)
ON UPDATE CASCADE -- if contract_id in `contracts` gets updated this will also get updated
ON DELETE CASCADE;

ALTER TABLE bills
ADD CONSTRAINT bills_unique_contract_id_month UNIQUE (contract_id, month);

-- `payment_details` table
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
    v_duration INT;
    v_start_month CHAR(7);
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

    -- check if the apartment has already been under a contract at the first day of start_month
    IF EXISTS (
        SELECT 1 
        FROM contracts C
        JOIN requests R ON R.request_id = C.request_id
        WHERE R.apartment_id = NEW.apartment_id
            AND C.end_date >= TO_DATE(NEW.start_month || '-01', 'YYYY-MM-DD')
    ) THEN 
        RAISE EXCEPTION 'Cannot request for apartment from start_month = %, it is being rented', NEW.start_month;
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
CREATE OR REPLACE FUNCTION tf_bf_insert_on_contracts() 
RETURNS TRIGGER AS $$
DECLARE 
    v_start_date DATE;
    v_end_date DATE;
    v_apartment_id INT;
    v_loop RECORD; -- RECORD type holds the results of a query
BEGIN

    -- calculate start_date and end_date of this contract
    SELECT 
        TO_DATE(start_month || '-01', 'YYYY-MM-DD'), 
        ((TO_DATE(start_month || '-01', 'YYYY-MM-DD') +  (duration || ' months')::INTERVAL) - INTERVAL '1 day')::DATE,
        apartment_id
    INTO v_start_date, v_end_date, v_apartment_id
    FROM requests
    WHERE request_id = NEW.request_id;

    -- set start_date and end_date column in the insert query 
    NEW.start_date = v_start_date;
    NEW.end_date = v_end_date;
    -- note that this will set the value of start_date and end_date
    -- in the insert query correspondingly whether or not we specify
    -- these 2 columns in the INSERT query

    -- loop through existing contracts to check if there is any overlap between
    -- [start_date:end_date] of any contract and [start_date:end_date]
    -- of to-be-created contract (to-be-accepted request)
    FOR v_loop IN (
        SELECT start_date, end_date 
        FROM contracts C 
        JOIN requests R ON R.request_id = C.request_id
        WHERE R.apartment_id = v_apartment_id
    ) LOOP 
        IF (v_loop.start_date <= NEW.end_date AND v_loop.end_date >= NEW.start_date) THEN 
            RAISE EXCEPTION 'cannot accept request %, apartment is being rented between % and %', NEW.request_id, v_loop.start_date, v_loop.end_date;
        END IF;
    END LOOP;

    -- if all conditions satisfy then insert this new record into contracts
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