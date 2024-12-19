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
    bill_id INT PRIMARY KEY,
    contract_id INT,
    month INT,
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
ON DELETE SET NULL;

-- `payment_detals` table
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

---------------------- TRIGGER CONSTRAINTS ------------------------
-------------------------------------------------------------------

-- `requests` table
-- insert constraints: if tenants want to craete a new request
--
CREATE OR REPLACE FUNCTION tf_bf_insert_on_requests()
RETURNS TRIGGER AS $$
DECLARE
    v_duration INT;
    v_start_month CHAR(7);
BEGIN

    -- check if duration is greater or equal than 3 or not
    IF(NEW.duration < 3) THEN 
        RAISE EXCEPTION 'duration = % is less than 3, cannot add request', NEW.duration; -- RAISE EXCEPTION stops the execution of function immediately, no need for return NULL 
        -- RETURN NULL;
    END IF;

    -- start_month must be after request_date
    IF NEW.start_month <= TO_CHAR(NEW.request_date, 'YYYY-MM') THEN
        RAISE EXCEPTION 'Start month must be after request_date';
    END IF;

    -- start_month must not be more than 1 year
    -- compared to request_date
    IF ( (NEW.request_date + INTERVAL '1 year') < (TO_DATE(NEW.start_month || '-01', 'YYYY-MM-DD')) ) THEN
        RAISE EXCEPTION 'Can only request within 1 year of current_date';
    END IF;

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
-- delete contraint: if tenant wants to cancel a created request 
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
    v_loop RECORD; -- RECORD type holds the results of a query
BEGIN

    -- calculate start_date and end_date of this contract
    SELECT TO_DATE(start_month || '-01', 'YYYY-MM-DD'), ((TO_DATE(start_month || '-01', 'YYYY-MM-DD') +  (duration || ' months')::INTERVAL) - INTERVAL '1 day')::DATE
    INTO v_start_date, v_end_date
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
        WHERE R.apartment_id = (
            SELECT apartment_id 
            FROM requests 
            WHERE request_id = NEW.request_id
        )
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

-- `bills` table
-- insert constraint: cannot have more than 1 bill for each month of the contract
--
CREATE OR REPLACE FUNCTION tf_bf_insert_on_bills()
RETURNS TRIGGER AS $$
BEGIN

    -- check if bill is already generated for that month of that contract
    IF EXISTS (
        SELECT 1 
        FROM bills 
        WHERE contract_id = NEW.contract_id
            AND month = NEW.month
    ) THEN 
        RAISE EXCEPTION 'already added bill for that month for contract_id %', NEW.contract_id;
    END IF;

    -- if ok
    RETURN NEW;

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bf_insert_on_bills
BEFORE INSERT ON bills
FOR EACH ROW
EXECUTE PROCEDURE tf_bf_insert_on_bills();

-- `payment_details` table
-- insert constraint: when tenants pay for a bill
-- 
CREATE OR REPLACE FUNCTION tf_bf_insert_on_payment_details()
RETURNS TRIGGER AS $$
BEGIN 

    -- tenant cannot pay more than once for the same bill
    IF EXISTS (
        SELECT 1 
        FROM payment_details 
        WHERE tenant_id = NEW.tenant_id
            AND bill_id = NEW.bill_id
    ) THEN 
        RAISE EXCEPTION 'Cannot add payment details, tenant % already paid for bill %', NEW.tenant_id, NEW.bill_id;
    END IF;

    RETURN NEW;

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bf_insert_on_payment_details
BEFORE INSERT ON payment_details
FOR EACH ROW 
EXECUTE PROCEDURE tf_bf_insert_on_payment_details();

-- `rating` table
-- insert constraint: when tenants want to rate an apartment
CREATE OR REPLACE FUNCTION tf_bf_insert_on_rating()
RETURNS TRIGGER AS $$
BEGIN 

    -- check if this tenant has already rated for this apartment before
    IF EXISTS (
        SELECT 1 
        FROM rating 
        WHERE tenant_id = NEW.tenant_id
            AND apartment_id = NEW.apartment_id
    ) THEN 
        RAISE EXCEPTION 'Cannot rate, tenant % alread rated apartment %', NEW.tenant_id, NEW.apartment_id;
    END IF;

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
BEFORE INSERT ON payment_details
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



------------------ POPULATING DATA to tables ----------------------
-------------------------------------------------------------------
-- NOTE: used your own path to sql files
\i /Users/hieuhoang/Desktop/Projects/apartments_management_system/data/tenants/tenants.sql
  
\i /Users/hieuhoang/Desktop/Projects/apartments_management_system/data/landlords/landlords.sql

\i /Users/hieuhoang/Desktop/Projects/apartments_management_system/data/apartments/apartments.sql

\i /Users/hieuhoang/Desktop/Projects/apartments_management_system/data/requests/requests.sql;