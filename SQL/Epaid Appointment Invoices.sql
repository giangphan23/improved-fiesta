-- Initial SQL
SET SESSION GROUP_CONCAT_MAX_LEN = 1000000;

-- Custom SQL
SELECT
  temp.*,
  IF(temp.clinic IN ('Phòng Khám Bác Sĩ Mỹ Vân', 'Phòng Khám Liên Kết Docosan')
  AND temp.original_service_type = 'at_home'
  AND temp.original_service LIKE '%PCR%', 'fixed', 'percent') AS cost_type,
  IF(temp.original_service_type = 'telemedicine',
  IF(temp.clinic NOT IN ('Phòng Khám Bác Sĩ Mỹ Vân', 'Phòng Khám Liên Kết Docosan'), 90, 0),
  IF(temp.clinic IN ('Phòng Khám Bác Sĩ Mỹ Vân', 'Phòng Khám Liên Kết Docosan') AND temp.original_service_type = 'at_home' AND temp.original_service LIKE '%PCR%', '', 90)) AS cost

FROM (SELECT
    a.id appointment_id,
    a.start_time appointment_time,
    c.name clinic,
    GROUP_CONCAT(DISTINCT i.partner_transaction_id ORDER BY i.id ASC SEPARATOR '|') AS epaid_id,
    GROUP_CONCAT(IF(i.type = 'appointment', JSON_LENGTH(IFNULL(i.item_detail ->> '$.items.services', i.item_detail ->> '$.items.sale_services')), NULL)) AS service_count,
    -- original
    GROUP_CONCAT(IF(i.type = 'appointment',
    CASE WHEN i.payment_method IN ('momo') THEN 'Momo' WHEN i.payment_method IN ('zalopay', 'zalopay_atm', 'zalopay_cc') THEN 'Zalopay' WHEN i.payment_method IN ('onepay', 'onepay_atm') THEN 'Onepay' WHEN i.payment_method IN ('bank_transfer', 'mpp_guaranteed', 'local_banking') THEN 'Bank transfer' WHEN i.payment_method IN ('free_paid') THEN 'Free' WHEN i.payment_method IN ('cash_on_delivery') THEN 'Cash' WHEN ISNULL(i.payment_method) THEN 'No info' ELSE i.payment_method END,
    NULL)) original_payment_method,
    i.created_at AS original_payment_date,
--     GROUP_CONCAT(IF(i.type = 'appointment', JSON_UNQUOTE(IFNULL(i.item_detail ->> '$.items.services[0].name', i.item_detail ->> '$.items.sale_services[0].name')), NULL)) original_service,
    GROUP_CONCAT(IF(i.type = 'appointment', JSON_UNQUOTE(IFNULL(i.item_detail ->> '$.items.services', i.item_detail ->> '$.items.sale_services')), NULL)) original_service,
    GROUP_CONCAT(IF(i.type = 'appointment', JSON_UNQUOTE(IFNULL(i.item_detail ->> '$.items.services[0].service_type', i.item_detail ->> '$.items.sale_services[0].service_type')), NULL)) original_service_type,
--     SUM(IF(i.type = 'appointment', IFNULL(i.item_detail ->> '$.items.services[0].price', i.item_detail ->> '$.items.sale_services[0].price'), NULL)) original_fee,
    SUM(IF(i.type = 'appointment', i.amount, NULL)) original_fee,
    GROUP_CONCAT(IF(i.type = 'appointment', i.status, NULL)) original_fee_status,
--     GROUP_CONCAT(
--     IF(i.type = 'appointment',
--     CONCAT_WS(' | ',
--     JSON_UNQUOTE(IFNULL(i.item_detail ->> '$.items.services[0].id', i.item_detail ->> '$.items.sale_services[0].id')),
--     JSON_UNQUOTE(IFNULL(i.item_detail ->> '$.items.services[0].name', i.item_detail ->> '$.items.sale_services[0].name')),
--     IF(NOT ISNULL(i.item_detail ->> '$.items.sale_services[0].name'), 'discounted', 'not discounted'),
--     IFNULL(i.item_detail ->> '$.items.services[0].price', i.item_detail ->> '$.items.sale_services[0].origin_price'),
--     IFNULL(i.item_detail ->> '$.items.services[0].price', i.item_detail ->> '$.items.sale_services[0].price'),
--     i.status,
--     CASE WHEN i.payment_method IN ('momo') THEN 'Momo' WHEN i.payment_method IN ('zalopay', 'zalopay_atm', 'zalopay_cc') THEN 'Zalopay' WHEN i.payment_method IN ('onepay', 'onepay_atm') THEN 'Onepay' WHEN i.payment_method IN ('bank_transfer', 'mpp_guaranteed', 'local_banking') THEN 'Bank transfer' WHEN i.payment_method IN ('free_paid') THEN 'Free' WHEN i.payment_method IN ('cash_on_delivery') THEN 'Cash' WHEN ISNULL(i.payment_method) THEN 'No info' ELSE i.payment_method END),
--     NULL)) AS original_fee_details,
    GROUP_CONCAT(IF(i.type = 'appointment', JSON_UNQUOTE(IFNULL(i.item_detail ->> '$.items.services', i.item_detail ->> '$.items.sale_services')), NULL)) original_fee_details,
    -- extra
    SUM(IF(i.type = 'extra_apt', i.item_detail ->> '$.items.services.extra.price', 0)) AS extra_fee_requested,
    SUM(IF(i.type = 'extra_apt' AND i.status = 'success', i.item_detail ->> '$.items.services.extra.price', 0)) AS extra_fee_paid,
    GROUP_CONCAT(
    IF(i.type = 'extra_apt',
    CONCAT_WS(' | ',
    JSON_UNQUOTE(i.item_detail ->> '$.items.services.extra.name'),
    i.item_detail ->> '$.items.services.extra.price',
    i.status,
    CASE WHEN i.payment_method IN ('momo') THEN 'Momo' WHEN i.payment_method IN ('zalopay', 'zalopay_atm', 'zalopay_cc') THEN 'Zalopay' WHEN i.payment_method IN ('onepay', 'onepay_atm') THEN 'Onepay' WHEN i.payment_method IN ('bank_transfer', 'mpp_guaranteed', 'local_banking') THEN 'Bank transfer' WHEN i.payment_method IN ('free_paid') THEN 'Free' WHEN i.payment_method IN ('cash_on_delivery') THEN 'Cash' WHEN ISNULL(i.payment_method) THEN 'No info' ELSE i.payment_method END),
    NULL)
    ORDER BY i.id ASC SEPARATOR ';\n') AS extra_fee_details

  FROM invoices i
    JOIN appointments a
      ON i.appointment_id = a.id
    JOIN clinics c
      ON a.clinic_id = c.id
    JOIN patients p
      ON a.patient_id = p.id

  WHERE i.type <> 'cares-order'
  AND a.request_by_providers = '0'
  AND NOT FIND_IN_SET(a.clinic_id, (SELECT
      list_user
    FROM user_exceptions
    WHERE type = 'clinic'))
  AND NOT FIND_IN_SET(a.doctor_id, (SELECT
      list_user
    FROM user_exceptions
    WHERE type = 'doctor'))
  AND NOT FIND_IN_SET(a.patient_id, (SELECT
      list_user
    FROM user_exceptions
    WHERE type = 'patient'))
  AND NOT (a.requester LIKE '%demo%')
  AND NOT (IF(ISNULL(a.symptom), '', a.symptom) LIKE '%demo%'
  OR IF(ISNULL(a.symptom), '', a.symptom) LIKE '%test booking%'
  OR IF(ISNULL(a.symptom), '', a.symptom) LIKE '%docosan%%testing'
  OR IF(ISNULL(a.symptom), '', a.symptom) LIKE '%docosan%%test%')
  GROUP BY i.appointment_id) temp
