--TRUNCATE rating, bills, payment_details;
--COPY rating FROM 'D:\\Documents\\HUST\\Database Lab\\Project\\apartments_management_system\\data\\rating\\rating.csv' DELIMITER ',' CSV HEADER;
--COPY bills FROM 'D:\\Documents\\HUST\\Database Lab\\Project\\apartments_management_system\\data\\bills\\bills.csv' DELIMITER ',' CSV HEADER;
--COPY payment_details FROM 'D:\\Documents\\HUST\\Database Lab\\Project\\apartments_management_system\\data\\payment_details\\payment_details.csv' DELIMITER ',' CSV HEADER;

--Query 10--
DROP FUNCTION IF EXISTS view_pending_requests, view_apartment_info, view_landlord_info, view_recived_requests,view_request_statistics, view_tenant_accept_rate;
DROP MATERIALIZED VIEW IF EXISTS apartment_avg_rating;

--CREATE INDEX idx_apartments_apartment_id ON apartments(apartment_id);
--CREATE INDEX idx_requests_apartment_id ON requests(apartment_id);
--CREATE INDEX idx_requests_tenant_id ON requests(tenant_id);
--CREATE INDEX idx_requests_request_id ON requests(request_id);
--CREATE INDEX idx_bills_bill_id ON bills(bill_id);

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

