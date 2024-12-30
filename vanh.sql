-- pg_restore -U postgres -d apartments_project 'D:\Database Lab\apartments_management_system\database_backup'
-------------------------------------------------------------------1
-- OK
create or replace function request_to_rent_apartment (IN tenantID integer, apartmentID integer, startMonth char(7), 
rentDuration integer) returns void
as
$$
declare
    newID integer;
begin
    select max(request_id) + 1 into newID from requests;
    insert into requests (request_id, tenant_id, apartment_id, request_date, start_month, duration)
    values (newID, tenantID, apartmentID, current_date, startMonth, rentDuration);

    raise notice 'Request %: Tenant % requests for apartment % for % months.', newID, tenantID, apartmentID, rentDuration;

end;
$$
language plpgsql
volatile;

-- see if tenant 500 has rent apartment 600 or not
select apartment_id, start_date, end_date, contract_id
from contracts c
join requests r on c.request_id = r.request_id
where tenant_id = 500;

-- inner query analysis
explain analyze
    insert into requests (request_id, tenant_id, apartment_id, request_date, start_month, duration)
    values (49995, 500, 600, current_date, '2025-09', 8);

-- delete the test request
delete from requests where request_id = 49995;


-- OK
-------------------------------3
create or replace function pay_monthly_bills (IN billID integer, IN tenantID integer) returns void
as
$$
declare
    billMonth char(7);
begin
    if (exists (
        select 1
        from payment_details
        where bill_id = billID
    )) then
        raise notice 'Bill % has been paid.', billID;
        return;
    end if;

    if (not exists (
        select 1
        from bills
        where bill_id = billID
    )) then
        raise notice 'Bill % is not created yet.', billID;
        return;
    end if;

    insert into payment_details (bill_id, tenant_id, payment_date)
    values (billID, tenantID, current_date);

    raise notice '%s: Bill %d successfully.', current_date, billID;
end;
$$
language plpgsql
volatile;

-- view unpaid bill
select bill_id, tenant_id, c.contract_id, start_date, end_date
from bills b left join contracts c on b.contract_id = c.contract_id
left join requests r on c.request_id = r.request_id
where bill_id not in (
    select bill_id from payment_details
);

-- delete test payment
delete from payment_details where bill_id = 28;


explain analyze
select 1
from payment_details
where bill_id = 28;
explain analyze
select 1 from bills
where bill_id = 28;
explain analyze
insert into payment_details (bill_id, tenant_id, payment_date)
values (28, 705, current_date);



-- CANOOT TEST
-------------------------------4
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
        raise notice 'Cannot cancel contract under 3 months from start date.';
        return;
    end if;

    if (extract(day from current_date) < 5) then
        raise notice 'Cnnot cancel contract sooner than 5th day of the month.';
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

select cancel_contract(705,28);

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
    where (startSize is null or size >= startSize)
      and (endSize is null or size <= endSize)
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
WHERE size >= 100
  AND size <= 120
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

-- see requests that have not been accepted
select *
from requests
where request_id not in (
    select request_id from contracts
);

-- analysis
explain analyze
    delete from requests
    where request_id = 31;

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


-- see ratings of tenant 477
select * from rating where tenant_id = 477;
-- delete rating of tenant 477 to apartment 548
delete from rating where tenant_id = 477 and apartment_id = 548;

-- analysis
explain analyze
    insert into requests (request_id, tenant_id, apartment_id, request_date, start_month, duration)
    values (49996,200,300,'2025-01-01','2025-02',4);

-- delete test rating
delete from requests where request_id = 49996;


-- OK
-------------------------------19
create or replace function view_total_money_on_apartment (IN tenantID integer, IN apartmentID integer) returns void
as
$$
declare
    totalMoney integer;
begin
    with tmp as (
        select b.bill_id, price
        from contracts c
        left join requests r on c.request_id = r.request_id
        left join bills b on c.contract_id = b.contract_id
        where r.tenant_id = tenantID and r.apartment_id = apartmentID
    )
    select coalesce(sum(price),0) into totalMoney
    from tmp
    where bill_id in (
        select bill_id from payment_details
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

-- analysis
explain analyze
    with tmp as (
        select b.bill_id, price
        from contracts c
        left join requests r on c.request_id = r.request_id
        left join bills b on c.contract_id = b.contract_id
        where r.tenant_id = 600 and r.apartment_id = 516
    )
    select coalesce(sum(price),0)
    from tmp
    where bill_id in (
        select bill_id from payment_details
    );


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

-- execute function
select * from view_monthly_expected_earning(600, '2022-10');

-- analysis
explain analyze
with tmp as (
        select contract_id
        from contracts c
        join requests r on c.request_id = r.request_id
        join apartments a on r.apartment_id = a.apartment_id
        where a.landlord_id = 600
    )
    select coalesce(sum(price),0)
    from tmp t join bills b on t.contract_id = b.contract_id
    where b.month = '2022-10';



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


-- execute function
select view_monthly_received_earning(500,'2022-11');

-- analysis
explain analyze 
with tmp as (
        select contract_id
        from contracts c
        join requests r on c.request_id = r.request_id
        join apartments a on r.apartment_id = a.apartment_id
        where a.landlord_id = 500
    )
    select coalesce(sum(price),0)
    from tmp t join bills b on t.contract_id = b.contract_id
    where b.month = '2022-11'
    and exists (
        select 1
        from payment_details p
        where b.bill_id = p.bill_id
    );

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

            if (v_loop2.payment_date is null and extract(day from current_date) > 10) then
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

EXPLAIN ANALYZE
DO $$
BEGIN
    PERFORM view_blacklist_tenants(1000);
END;
$$;


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


-- analysis
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


-- see request 631
select * from requests where request_id = 631;

-- analysis
explain analyze
select start_month from requests where request_id = 631;
explain analyze
select max(contract_id) + 1
from contracts;
explain analyze
insert into contracts (contract_id, rent_amount, request_id)
values (10001, 1000, 631);


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

-- execute function
select * from terminate_contract(400);

-- analysis
explain analyze
select b.bill_id, b.month, p.payment_date 
        from contracts c left join bills b on c.contract_id = b.contract_id
        left join payment_details p on b.bill_id = p.bill_id
        where c.contract_id = 400;


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

-- execute function
select * from view_payment_status(4);

-- analysis
explain analyze
select c.contract_id, b.bill_id, b.month, p.payment_date
    from contracts c join bills b on c.contract_id = b.contract_id
    left join payment_details p on b.bill_id = p.bill_id
    where c.contract_id = 4;


-- 
---------------------------------35
create or replace function add_apartment(IN aress varchar, IN sze integer, IN berooms integer, IN barooms integer, 
IN kchen varchar, IN aconditioner varchar, IN t_v varchar, IN landlordID integer)
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

select * from add_apartment('Hanoi002', 100, 2, 2, 'YES', 'YES', 'YES', 'YES', 100);

-- analysis
explain analyze 
select * from apartments where address = 'Hanoi002';





      










    
