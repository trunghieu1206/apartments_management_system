--
SELECT l.*
FROM landlords l
LEFT JOIN apartments a ON l.landlord_id = a.landlord_id
WHERE a.apartment_id IS NULL
ORDER BY l.landlord_id ASC;
--)
--TO 'D:\\Documents\\HUST\\Database Lab\\Project\\apartments_manage_system\\temp.csv'
--DELIMITER ',' csv HEADER;

--'D:\\Documents\\HUST\\Database Lab\\Project\\apartments_manage_system\\data\\apartments\\apartments.sql'
