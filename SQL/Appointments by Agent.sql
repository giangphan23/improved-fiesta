-- Active: 1661854745457@@docosandynamic.c1lsds8lkx7p.ap-southeast-1.rds.amazonaws.com@3306@docosandynamic
-- Initial SQL

-- Custom SQL
SELECT
  a.id AS appointment_id,
  mp.user_id AS agent_id,
  mp.display_name AS agent_name

FROM appointments a
  JOIN patients p
    ON a.patient_id = p.id
  JOIN event_logs el
    ON a.id = el.data_id
    AND el.action = 'created'
    AND el.sub_action = 'mpp_created'
    AND el.table_name = 'appointments'
  JOIN master_patient mp
    ON el.created_by = mp.user_id

WHERE a.request_by_providers = '0'
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
AND NOT (p.display_name IS NOT NULL
AND (p.display_name LIKE '%test%'
OR p.display_name LIKE '%demo%'))
AND NOT (p.address IS NOT NULL
AND (p.address LIKE '%test%'
OR p.address LIKE '%demo%'))
AND NOT (a.requester LIKE '%demo%')
AND NOT (IF(ISNULL(a.symptom), '', a.symptom) LIKE '%demo%'
OR IF(ISNULL(a.symptom), '', a.symptom) LIKE '%test booking%'
OR IF(ISNULL(a.symptom), '', a.symptom) LIKE '%docosan%%testing%'
OR IF(ISNULL(a.symptom), '', a.symptom) LIKE '%docosan%%test%')
