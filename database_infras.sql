-- REQUIREMENTS:
-- create a database named `apartments_project` 
-- connect to this database and then run the sql script

-- SOME NOTES:
    -- when declaring a column as UNIQUE, a BTREE index is automatically created for that column

-------------------------- TABLES ---------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS tenants, landlords, apartments, requests, contracts, bills, payment_details;

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
    tenant_id INT,
    contract_id INT,
    month INT,
    price INT NOT NULL
);

CREATE TABLE payment_details (
    bill_id INT,
    payment_date DATE NOT NULL
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
ADD CONSTRAINT bills_fk_tenants FOREIGN KEY (tenant_id)
REFERENCES tenants(tenant_id)
ON UPDATE CASCADE -- if tenant_id in `tenant` gets updated this will also get updated
ON DELETE SET NULL;

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

---------------------- TRIGGER CONSTRAINTS ------------------------
-------------------------------------------------------------------

-- `requests` table
-- insert constraints
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

    -- check if tenant has already requested for this apartment in that month
        -- more optimized query
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
            AND C.end_date >= TO_DATE(NEW.start_month || '-01', 'YYYY-MM-DD')::DATE
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
-- delete contraint
--
CREATE OR REPLACE FUNCTION tf_bf_delete_on_requests()
RETURNS TRIGGER AS $$
DECLARE 

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

CREATE TRIGGER bf_delete_on_requestes
BEFORE DELETE ON requests
FOR EACH ROW 
EXECUTE PROCEDURE tf_bf_delete_on_requests();

-- `contracts` table
-- insert constraint
-- 
CREATE OR REPLACE FUNCTION tf_bf_insert_on_contracts() 
RETURNS TRIGGER AS $$
DECLARE

BEGIN

    -- check if end_date - start_date >= 3 or not
    IF(EXTRACT(MONTH FROM AGE(NEW.end_date + INTERVAL '1 day', NEW.start_date)) < 3) THEN
        RAISE EXCEPTION 'Rent duration is not greater or equal than 3';
    END IF;

    -- check if the apartment is still being rented until start_date
    IF EXISTS (
        SELECT 1 
        FROM contracts C
        JOIN requests R ON R.apartment_id = C.apartment_id
        WHERE R.apartment_id = NEW.apartment_id
            AND C.end_date >= NEW.start_date
    ) THEN
        RAISE EXCEPTION 'cannot accept request, apartment is being rented at %', NEW.start_date;
    END IF;

    -- if all conditions satisfy then proceed
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
DECLARE

BEGIN

    -- not yet implement

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bf_update_on_contracts
BEFORE UPDATE ON contracts
FOR EACH ROW
EXECUTE PROCEDURE tf_bf_update_on_contracts();