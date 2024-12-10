-- REQUIREMENTS:
-- create a database named `apartments_project` 
-- connect to this database and then run the sql script

-------------------------- TABLES ---------------------------------
-------------------------------------------------------------------
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
    tenant_id INT,
    apartment_id INT,
    request_date DATE,
    start_month CHAR(7) NOT NULL, -- for e.g: 2024-12 or 2025-01 
    duration INT NOT NULL
);

CREATE TABLE contracts (
    contract_id INT PRIMARY KEY,
    tenant_id INT,
    apartment_id INT,
    sign_date DATE NOT NULL,
    end_date DATE NOT NULL,
    rent_amount INT NOT NULL
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
-- tenants table
ALTER TABLE tenants
ADD CONSTRAINT unique_email UNIQUE(email);

ALTER TABLE tenants 
ADD CONSTRAINT unique_phone UNIQUE(phone);

-- landlords table
ALTER TABLE landlords
ADD CONSTRAINT unique_email UNIQUE(email);

ALTER TABLE landlords 
ADD CONSTRAINT unique_phone UNIQUE(phone);

-- apartments table
ALTER TABLE apartments
ADD CONSTRAINT unique_address UNIQUE(address)

ALTER TABLE apartments
ADD CONSTRAINT apartments_fk_landlords FOREIGN KEY (landlord_id)
REFERENCES landlords(landlord_id)
ON UPDATE CASCADE 
ON DELETE CASCADE;

-- requests table
ALTER TABLE requests 
ADD CONSTRAINT pk_requests PRIMARY KEY (tenant_id, apartment_id, request_date);

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

-- contracts table
ALTER TABLE contracts
ADD CONSTRAINT contracts_fk_tenants FOREIGN KEY (tenant_id)
REFERENCES tenants(tenant_id)
ON UPDATE CASCADE -- if tenant_id in `tenants` gets updated this will also get updated
ON DELETE SET NULL;

ALTER TABLE contracts
ADD CONSTRAINT contracts_fk_apartments FOREIGN KEY (apartment_id)
REFERENCES apartments(apartment_id)
ON UPDATE CASCADE -- if apartment_id in `apartments` gets updated this will also get updated
ON DELETE SET NULL;

-- bills table
ALTER TABLE bills
ADD CONSTRAINT bills_fk_tenants FOREIGN KEY (tenant_id)
REFERENCES tenants(tenant_id)
ON UPDATE CASCADE -- if tenant_id in `tenant` gets updated this will also get updated
ON DELETE SET NULL;

ALTER TABLE bills
ADD CONSTRAINT bills_fk_contracts FOREIGN KEY (contract_id)
REFERENCES contracts(contract_id);
ON UPDATE CASCADE -- if contract_id in `contracts` gets updated this will also get updated
ON DELETE SET NULL;

-- payment_detals table
ALTER TABLE payment_details
ADD CONSTRAINT payment_details_fk_bills FOREIGN KEY (bill_id)
REFERENCES bills(bill_id)
ON UPDATE CASCADE -- if bill_id gets updated in `bills` this will get updated
ON DELETE CASCADE;  -- if bill is deleted payment_details also get deleted

---------------------- TRIGGER CONSTRAINTS ------------------------
-------------------------------------------------------------------


