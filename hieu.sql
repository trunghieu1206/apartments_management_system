---------------------------------------------------------------------
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
-- (already with composite B-tree index on (bill_id, month) of bills table)
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

---------------------------------------------------------------------
-- *****
-- `requests` table
-- insert constraints: if tenants want to create a new request
--
CREATE OR REPLACE FUNCTION tf_bf_insert_on_requests()
RETURNS TRIGGER AS $$
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

-- auto add bill function, can be called using pg_cron extension to auto check for 5th day
-- 
CREATE OR REPLACE FUNCTION generate_bills()
RETURNS VOID AS $$
DECLARE 
    v_contract_id INT;
    v_rent_amount INT;
BEGIN 

    INSERT INTO bills (contract_id, month, price)
    SELECT contract_id, EXTRACT(MONTH FROM CURRENT_DATE), rent_amount
    FROM contracts
    WHERE end_date > CURRENT_DATE;

END;
$$ LANGUAGE plpgsql;

---------------------------------------------------------------------
-- SOME NOTES:
-- When to use index
    -- for large table, frequent query to retrieve data
    -- joining tables to increase performance
    -- conditions on columns with high-selectivity 
-- When not to use index
    -- for SMALL TABLES, seq scan (whole scan on table) could be faster than having to maintain index overhead
    -- for tables with frequent insertions, updates and deletes, we have to consider the tradeoff between these operations and queries (If frequent queries then index is still useful)
    -- when the filter condition returns a large number of matching rows then index might not prove to be beneficial (low cardinality columns)
