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