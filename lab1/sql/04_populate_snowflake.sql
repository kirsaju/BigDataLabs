-- 04_populate_snowflake.sql
-- Populate snowflake schema from raw_data.
-- Order: sub-dimensions -> dimensions -> fact table.
-- All batch INSERT INTO ... SELECT DISTINCT ... operations.

-- ============================================================
-- SECOND-LEVEL SUB-DIMENSIONS
-- ============================================================

-- d_customer_location: deduplicate by (country, postal_code)
INSERT INTO d_customer_location (country, postal_code)
SELECT DISTINCT
    COALESCE(customer_country, ''),
    COALESCE(customer_postal_code, '')
FROM raw_data;

-- d_customer_pet: deduplicate by (pet_type, pet_name, pet_breed, pet_category)
INSERT INTO d_customer_pet (pet_type, pet_name, pet_breed, pet_category)
SELECT DISTINCT
    COALESCE(customer_pet_type, ''),
    COALESCE(customer_pet_name, ''),
    COALESCE(customer_pet_breed, ''),
    COALESCE(pet_category, '')
FROM raw_data;

-- d_seller_location: deduplicate by (country, postal_code)
INSERT INTO d_seller_location (country, postal_code)
SELECT DISTINCT
    COALESCE(seller_country, ''),
    COALESCE(seller_postal_code, '')
FROM raw_data;

-- d_store_location: deduplicate by (city, state, country)
INSERT INTO d_store_location (city, state, country)
SELECT DISTINCT
    COALESCE(store_city, ''),
    COALESCE(store_state, ''),
    COALESCE(store_country, '')
FROM raw_data;

-- d_supplier_location: deduplicate by (address, city, country)
INSERT INTO d_supplier_location (address, city, country)
SELECT DISTINCT
    COALESCE(supplier_address, ''),
    COALESCE(supplier_city, ''),
    COALESCE(supplier_country, '')
FROM raw_data;

-- d_product_category: deduplicate by category_name
INSERT INTO d_product_category (category_name)
SELECT DISTINCT
    COALESCE(product_category, '')
FROM raw_data;

-- d_product_details: deduplicate by all detail fields
INSERT INTO d_product_details (weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date)
SELECT DISTINCT
    CASE WHEN product_weight ~ '^\d+(\.\d+)?$' THEN product_weight::NUMERIC ELSE NULL END,
    COALESCE(product_color, ''),
    COALESCE(product_size, ''),
    COALESCE(product_brand, ''),
    COALESCE(product_material, ''),
    COALESCE(product_description, ''),
    CASE WHEN product_rating ~ '^\d+(\.\d+)?$' THEN product_rating::NUMERIC ELSE NULL END,
    CASE WHEN product_reviews ~ '^\d+$' THEN product_reviews::INT ELSE NULL END,
    CASE WHEN product_release_date <> '' AND product_release_date IS NOT NULL
         THEN TO_DATE(product_release_date, 'MM/DD/YYYY') ELSE NULL END,
    CASE WHEN product_expiry_date <> '' AND product_expiry_date IS NOT NULL
         THEN TO_DATE(product_expiry_date, 'MM/DD/YYYY') ELSE NULL END
FROM raw_data;

-- d_date: deduplicate by unique date values from sale_date
INSERT INTO d_date (full_date, day, month, year, quarter)
SELECT DISTINCT
    TO_DATE(sale_date, 'MM/DD/YYYY'),
    EXTRACT(DAY   FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT,
    EXTRACT(MONTH FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT,
    EXTRACT(YEAR  FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT,
    EXTRACT(QUARTER FROM TO_DATE(sale_date, 'MM/DD/YYYY'))::INT
FROM raw_data
WHERE sale_date IS NOT NULL AND sale_date <> '';

-- ============================================================
-- FIRST-LEVEL DIMENSIONS
-- ============================================================

-- d_customer: deduplicate by (first_name, last_name, email), join to sub-dims
INSERT INTO d_customer (first_name, last_name, age, email, pet_id, location_id)
SELECT
    c.customer_first_name,
    c.customer_last_name,
    CASE WHEN c.customer_age ~ '^\d+$' THEN c.customer_age::INT ELSE NULL END,
    c.customer_email,
    cp.pet_id,
    cl.location_id
FROM (
    SELECT DISTINCT ON (customer_first_name, customer_last_name, customer_email)
        customer_first_name,
        customer_last_name,
        customer_age,
        customer_email,
        COALESCE(customer_country, '')      AS customer_country,
        COALESCE(customer_postal_code, '')  AS customer_postal_code,
        COALESCE(customer_pet_type, '')     AS customer_pet_type,
        COALESCE(customer_pet_name, '')     AS customer_pet_name,
        COALESCE(customer_pet_breed, '')    AS customer_pet_breed,
        COALESCE(pet_category, '')          AS pet_category
    FROM raw_data
    ORDER BY customer_first_name, customer_last_name, customer_email
) c
JOIN d_customer_location cl
    ON cl.country     = c.customer_country
   AND cl.postal_code = c.customer_postal_code
JOIN d_customer_pet cp
    ON cp.pet_type     = c.customer_pet_type
   AND cp.pet_name     = c.customer_pet_name
   AND cp.pet_breed    = c.customer_pet_breed
   AND cp.pet_category = c.pet_category;

-- d_seller: deduplicate by (first_name, last_name, email), join to sub-dim
INSERT INTO d_seller (first_name, last_name, email, location_id)
SELECT
    s.seller_first_name,
    s.seller_last_name,
    s.seller_email,
    sl.location_id
FROM (
    SELECT DISTINCT ON (seller_first_name, seller_last_name, seller_email)
        seller_first_name,
        seller_last_name,
        seller_email,
        COALESCE(seller_country, '')     AS seller_country,
        COALESCE(seller_postal_code, '') AS seller_postal_code
    FROM raw_data
    ORDER BY seller_first_name, seller_last_name, seller_email
) s
JOIN d_seller_location sl
    ON sl.country     = s.seller_country
   AND sl.postal_code = s.seller_postal_code;

-- d_product: deduplicate by (product_name, product_category), join to sub-dims
INSERT INTO d_product (name, price, quantity, category_id, details_id)
SELECT
    p.product_name,
    CASE WHEN p.product_price ~ '^\d+(\.\d+)?$' THEN p.product_price::NUMERIC ELSE NULL END,
    CASE WHEN p.product_quantity ~ '^\d+$' THEN p.product_quantity::INT ELSE NULL END,
    pc.category_id,
    pd.details_id
FROM (
    SELECT DISTINCT ON (product_name, product_category)
        product_name,
        product_category,
        product_price,
        product_quantity,
        COALESCE(product_category, '')    AS cat_coalesced,
        CASE WHEN product_weight ~ '^\d+(\.\d+)?$' THEN product_weight::NUMERIC ELSE NULL END AS pw,
        COALESCE(product_color, '')       AS pcolor,
        COALESCE(product_size, '')        AS psize,
        COALESCE(product_brand, '')       AS pbrand,
        COALESCE(product_material, '')    AS pmat,
        COALESCE(product_description, '') AS pdesc,
        CASE WHEN product_rating ~ '^\d+(\.\d+)?$' THEN product_rating::NUMERIC ELSE NULL END AS prat,
        CASE WHEN product_reviews ~ '^\d+$' THEN product_reviews::INT ELSE NULL END AS prev,
        CASE WHEN product_release_date <> '' AND product_release_date IS NOT NULL
             THEN TO_DATE(product_release_date, 'MM/DD/YYYY') ELSE NULL END AS prel,
        CASE WHEN product_expiry_date <> '' AND product_expiry_date IS NOT NULL
             THEN TO_DATE(product_expiry_date, 'MM/DD/YYYY') ELSE NULL END AS pexp
    FROM raw_data
    ORDER BY product_name, product_category
) p
JOIN d_product_category pc
    ON pc.category_name = p.cat_coalesced
JOIN d_product_details pd
    ON pd.color     = p.pcolor
   AND pd.size      = p.psize
   AND pd.brand     = p.pbrand
   AND pd.material  = p.pmat
   AND pd.description = p.pdesc
   AND (pd.weight     IS NOT DISTINCT FROM p.pw)
   AND (pd.rating     IS NOT DISTINCT FROM p.prat)
   AND (pd.reviews    IS NOT DISTINCT FROM p.prev)
   AND (pd.release_date IS NOT DISTINCT FROM p.prel)
   AND (pd.expiry_date  IS NOT DISTINCT FROM p.pexp);

-- d_store: deduplicate by (store_name, store_city), join to sub-dim
INSERT INTO d_store (name, location_detail, phone, email, location_id)
SELECT
    st.store_name,
    st.store_location,
    st.store_phone,
    st.store_email,
    stl.location_id
FROM (
    SELECT DISTINCT ON (store_name, store_city)
        store_name,
        COALESCE(store_location, '')  AS store_location,
        store_phone,
        store_email,
        COALESCE(store_city, '')      AS store_city,
        COALESCE(store_state, '')     AS store_state,
        COALESCE(store_country, '')   AS store_country
    FROM raw_data
    ORDER BY store_name, store_city
) st
JOIN d_store_location stl
    ON stl.city    = st.store_city
   AND stl.state   = st.store_state
   AND stl.country = st.store_country;

-- d_supplier: deduplicate by (supplier_name, supplier_email), join to sub-dim
INSERT INTO d_supplier (name, contact, email, phone, location_id)
SELECT
    su.supplier_name,
    su.supplier_contact,
    su.supplier_email,
    su.supplier_phone,
    sul.location_id
FROM (
    SELECT DISTINCT ON (supplier_name, supplier_email)
        supplier_name,
        supplier_contact,
        supplier_email,
        supplier_phone,
        COALESCE(supplier_address, '') AS supplier_address,
        COALESCE(supplier_city, '')    AS supplier_city,
        COALESCE(supplier_country, '') AS supplier_country
    FROM raw_data
    ORDER BY supplier_name, supplier_email
) su
JOIN d_supplier_location sul
    ON sul.address = su.supplier_address
   AND sul.city    = su.supplier_city
   AND sul.country = su.supplier_country;

-- ============================================================
-- FACT TABLE
-- ============================================================

-- f_sale: one row per raw_data row, joining to all dimensions by natural keys
INSERT INTO f_sale (sale_quantity, sale_total_price, customer_id, seller_id, product_id, store_id, supplier_id, date_id)
SELECT
    CASE WHEN r.sale_quantity ~ '^\d+$' THEN r.sale_quantity::INT ELSE NULL END,
    CASE WHEN r.sale_total_price ~ '^\d+(\.\d+)?$' THEN r.sale_total_price::NUMERIC ELSE NULL END,
    dc.customer_id,
    ds.seller_id,
    dp.product_id,
    dst.store_id,
    dsu.supplier_id,
    dd.date_id
FROM raw_data r
JOIN d_customer dc
    ON dc.first_name = r.customer_first_name
   AND dc.last_name  = r.customer_last_name
   AND dc.email      = r.customer_email
JOIN d_seller ds
    ON ds.first_name = r.seller_first_name
   AND ds.last_name  = r.seller_last_name
   AND ds.email      = r.seller_email
JOIN d_product dp
    ON dp.name = r.product_name
   AND dp.category_id = (
       SELECT pc.category_id FROM d_product_category pc
       WHERE pc.category_name = COALESCE(r.product_category, '')
   )
JOIN d_store dst
    ON dst.name = r.store_name
   AND dst.location_id = (
       SELECT stl.location_id FROM d_store_location stl
       WHERE stl.city    = COALESCE(r.store_city, '')
         AND stl.state   = COALESCE(r.store_state, '')
         AND stl.country = COALESCE(r.store_country, '')
   )
JOIN d_supplier dsu
    ON dsu.name  = r.supplier_name
   AND dsu.email = r.supplier_email
JOIN d_date dd
    ON dd.full_date = TO_DATE(r.sale_date, 'MM/DD/YYYY');

-- ============================================================
-- VERIFICATION
-- ============================================================

DO $$
DECLARE
    cnt BIGINT;
BEGIN
    SELECT COUNT(*) INTO cnt FROM raw_data;
    RAISE NOTICE 'raw_data rows:           %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_customer_location;
    RAISE NOTICE 'd_customer_location rows: %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_customer_pet;
    RAISE NOTICE 'd_customer_pet rows:      %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_seller_location;
    RAISE NOTICE 'd_seller_location rows:   %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_store_location;
    RAISE NOTICE 'd_store_location rows:    %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_supplier_location;
    RAISE NOTICE 'd_supplier_location rows: %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_product_category;
    RAISE NOTICE 'd_product_category rows:  %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_product_details;
    RAISE NOTICE 'd_product_details rows:   %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_customer;
    RAISE NOTICE 'd_customer rows:          %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_seller;
    RAISE NOTICE 'd_seller rows:            %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_product;
    RAISE NOTICE 'd_product rows:           %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_store;
    RAISE NOTICE 'd_store rows:             %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_supplier;
    RAISE NOTICE 'd_supplier rows:          %', cnt;
    SELECT COUNT(*) INTO cnt FROM d_date;
    RAISE NOTICE 'd_date rows:              %', cnt;
    SELECT COUNT(*) INTO cnt FROM f_sale;
    RAISE NOTICE 'f_sale rows:              %', cnt;
END $$;
