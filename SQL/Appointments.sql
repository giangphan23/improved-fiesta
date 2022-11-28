-- Active: 1665389269043@@docosandynamic.c1lsds8lkx7p.ap-southeast-1.rds.amazonaws.com@3306@docosandynamic
-- Initial SQL
CALL proc_appointments_info('last 3 days', null, null, 'No');
DROP TEMPORARY TABLE IF EXISTS last_changed_status_event;
CREATE TEMPORARY TABLE IF NOT EXISTS last_changed_status_event (
  id bigint(20),
  status varchar(50) CHARACTER SET utf8mb4,
  user_type varchar(20) CHARACTER SET utf8mb4
);
INSERT INTO last_changed_status_event
  SELECT
    a.id,
    CASE WHEN
        CONCAT_WS(' -> ', el.data_changed ->> '$.status.original', el.data_changed ->> '$.status.changes') IN ('request -> reject', 'on-hold -> reject') THEN IF(u.type LIKE '%patient%',
          'Patient cancelled (before confirmed)',
          'Doctor rejected') WHEN
        CONCAT_WS(' -> ', el.data_changed ->> '$.status.original', el.data_changed ->> '$.status.changes') = 'approve -> reject' THEN IF(u.type LIKE '%patient%',
          'Patient cancelled (after confirmed)',
          'Doctor cancelled') ELSE 'Doctor rejected' END status,
    u.type user_type
  FROM event_logs el
    JOIN appointments a
      ON a.id = el.data_id
      AND el.table_name = 'appointments'
      AND a.status = 'reject'
    JOIN (SELECT
        el1.data_id,
        MAX(el1.id) max_id
      FROM event_logs el1
      WHERE el1.table_name = 'appointments'
      AND el1.sub_action LIKE '%changed_status%'
      GROUP BY el1.data_id) el1
      ON el.data_id = el1.data_id
      AND el.id = el1.max_id
    LEFT JOIN users u
      ON el.created_by = u.id
  WHERE NOT FIND_IN_SET(el.data_id, @auto_rejected_apts);

DROP TEMPORARY TABLE IF EXISTS last_changed_status_event1;
CREATE TEMPORARY TABLE IF NOT EXISTS last_changed_status_event1
SELECT
  *
FROM last_changed_status_event lcse;

DROP TEMPORARY TABLE IF EXISTS appointment_statuses;
CREATE TEMPORARY TABLE IF NOT EXISTS appointment_statuses
SELECT
  a.id,
  IF(a.status = 'approve',
  CASE WHEN a.is_show = '1' THEN 'Show' WHEN a.is_show = '0' THEN 'No Show' ELSE 'Pending' END,
  '') show_status,
  CASE WHEN a.status = 'approve' THEN 'Confirmed' WHEN
      a.status = 'request' OR
      a.status = 'on-hold' THEN 'Unconfirmed' WHEN
      a.status = 'reject' THEN CASE WHEN FIND_IN_SET(a.id, @auto_rejected_apts) > 0 THEN 'Auto rejected' WHEN FIND_IN_SET(a.id, @event_logged_apts) <= 0 THEN 'Doctor rejected' ELSE IF(a.memo LIKE '%auto_rejected%', 'Auto rejected', 'Patient cancelled (before confirmed)') END ELSE a.status COLLATE 'utf8mb4_unicode_ci' END request_status,
  '' AS user_type
FROM appointments a
WHERE a.id NOT IN (SELECT
    lcse1.id
  FROM last_changed_status_event1 lcse1)
UNION
SELECT
  a.id,
  '' AS show_status,
  CASE WHEN lcse.status = 'Patient cancelled (before confirmed)' AND
      a.memo LIKE '%auto_rejected%' THEN 'Auto rejected'
      WHEN lcse.status = 'Patient cancelled (after confirmed)' AND
      a.memo LIKE '%auto_rejected%' THEN 'Auto rejected'
      ELSE lcse.status
      END request_status,
  lcse.user_type
FROM appointments a
  JOIN last_changed_status_event lcse
    ON a.id = lcse.id;

-- Custom SQL
SELECT
  a.id AS 'Appointment ID',
  c.id AS 'Clinic ID',
  c.name AS 'Clinic Name',
  c1.id AS 'Cluster ID',
  c1.name AS 'Cluster Name',
  d.id AS 'Doctor ID',
  d.display_name AS 'Doctor Name',
  p.id AS 'Patient ID',
  p.created_at "Patient Created At",
  p.display_name AS 'Patient Name',
  a.requester AS 'Requester',
  p.birthday AS 'Patient Birthday',
  IF(a.for_child IS NOT NULL, (SELECT
      CASE WHEN patients_childs.gender = 1 THEN 'Male' WHEN patients_childs.gender = 2 THEN 'Female' WHEN patients_childs.gender = 3 THEN 'Other' END
    FROM patients_childs
    WHERE patients_childs.id = a.for_child),
  CASE WHEN p.gender = 1 THEN 'Male' WHEN p.gender = 2 THEN 'Female' WHEN p.gender = 3 THEN 'Other' END) AS 'Patient Gender',
  u.phone_number AS 'Phone Number',
  u.email AS 'Email',
  a.symptom AS 'Reason',
  a.status AS 'Raw Status',
  `as`.request_status AS 'Status',
  `as`.user_type,
  JSON_UNQUOTE(JSON_EXTRACT(IF(NOT a.memo LIKE '[%', JSON_ARRAY(a.memo), a.memo), '$[0]')) 'Memo',
  FIND_IN_SET(a.id, @auto_rejected_apts) > 0 AND a.status = 'reject' AS 'Auto rejected',
  `as`.show_status AS 'Show/No-show',
  a.created_at AS 'Create Date',
  a.updated_at AS 'Updated At',
  DATE(a.start_time) AS 'Appointment Date',
  DATE_FORMAT(a.start_time, '%H:%i:%s') AS 'Appointment Time',
  a.type AS 'Appointment Type',
  a.mode AS 'Appointment Mode'
FROM appointments a
  JOIN appointment_statuses `as`
    ON a.id = `as`.id
  LEFT JOIN patients p
    ON p.id = a.patient_id
  LEFT JOIN clinics c
    ON c.id = a.clinic_id
  LEFT JOIN cluster_clinic_relationship ccr
    ON c.id = ccr.clinic_id
  LEFT JOIN clusters c1
    ON ccr.cluster_id = c1.id
  LEFT JOIN users u
    ON p.user_id = u.id
  LEFT JOIN doctors d
    ON d.id = a.doctor_id
WHERE
  NOT FIND_IN_SET(a.clinic_id, (SELECT
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

-- CASE
--   WHEN <Parameters.Include test and demo> = 'No' THEN
--     a.request_by_providers = '0'
--     AND NOT FIND_IN_SET(a.clinic_id, (SELECT
--         list_user
--       FROM user_exceptions
--       WHERE type = 'clinic'))
--     AND NOT FIND_IN_SET(a.doctor_id, (SELECT
--         list_user
--       FROM user_exceptions
--       WHERE type = 'doctor'))
--     AND NOT FIND_IN_SET(a.patient_id, (SELECT
--         list_user
--       FROM user_exceptions
--       WHERE type = 'patient'))
--     AND NOT (p.display_name IS NOT NULL
--     AND (p.display_name LIKE '%test%'
--     OR p.display_name LIKE '%demo%'))
--     AND NOT (p.address IS NOT NULL
--     AND (p.address LIKE '%test%'
--     OR p.address LIKE '%demo%'))
--     AND NOT (a.requester LIKE '%demo%')
--     AND NOT (a.symptom LIKE '%demo%'
--     OR a.symptom LIKE '%test booking%'
--     OR a.symptom LIKE '%docosan%%testing%'
--     OR a.symptom LIKE '%docosan%%test%')
--   ELSE TRUE
-- END
-- AND DATE_FORMAT(a.start_time, '%y%m') <= DATE_FORMAT(CURRENT_DATE(), '%y%m')
-- AND
-- CASE
--   WHEN <Parameters.Load appointment from> = 'yesterday' THEN a.start_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) -- AND a.start_time <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'yesterday only' THEN DATE(a.start_time) = DATE(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
--   WHEN <Parameters.Load appointment from> = 'last 3 days' THEN a.start_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) -- AND a.start_time <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'this week' THEN a.start_time >= DATE_SUB(CURRENT_DATE(), INTERVAL WEEKDAY(CURRENT_DATE()) DAY) -- AND a.start_time <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'last week' THEN a.start_time >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL WEEKDAY(CURRENT_DATE()) DAY), INTERVAL 1 WEEK) -- AND a.start_time <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'lastweek only' THEN a.start_time >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL WEEKDAY(CURRENT_DATE()) DAY), INTERVAL 1 WEEK) AND a.start_time < DATE_SUB(CURRENT_DATE(), INTERVAL WEEKDAY(CURRENT_DATE()) DAY)
--   WHEN <Parameters.Load appointment from> = 'this month' THEN a.start_time >= DATE_SUB(CURRENT_DATE(), INTERVAL DAY(CURRENT_DATE()) - 1 DAY) -- AND a.start_time <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'last month' THEN a.start_time >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL DAY(CURRENT_DATE()) - 1 DAY), INTERVAL 1 MONTH) -- AND a.start_time <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'last month only' THEN a.start_time >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL DAY(CURRENT_DATE()) - 1 DAY), INTERVAL 1 MONTH) AND a.start_time < DATE_SUB(CURRENT_DATE(), INTERVAL DAY(CURRENT_DATE()) - 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'last 3 months' THEN a.start_time >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL DAY(CURRENT_DATE()) - 1 DAY), INTERVAL 2 MONTH) -- AND a.start_time <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'last 6 months' THEN a.start_time >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL DAY(CURRENT_DATE()) - 1 DAY), INTERVAL 5 MONTH) -- AND a.start_time <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'this year' THEN a.start_time >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL DAY(CURRENT_DATE()) - 1 DAY), INTERVAL MONTH(CURRENT_DATE()) - 1 MONTH) -- AND a.start_time <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'last year' THEN a.start_time >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL DAY(CURRENT_DATE()) - 1 DAY), INTERVAL MONTH(CURRENT_DATE()) + 11 MONTH) -- AND a.start_time <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'last year only' THEN a.start_time >= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL DAY(CURRENT_DATE()) - 1 DAY), INTERVAL MONTH(CURRENT_DATE()) + 11 MONTH) AND a.start_time < DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL DAY(CURRENT_DATE()) - 1 DAY), INTERVAL MONTH(CURRENT_DATE()) - 1 MONTH)
--   WHEN <Parameters.Load appointment from> = 'custom' THEN a.start_time BETWEEN <Parameters.Start date> AND DATE_ADD(<Parameters.End date>, INTERVAL 1 DAY)
--   WHEN <Parameters.Load appointment from> = 'all' THEN TRUE
-- END
-- ORDER BY a.start_time ASC, 'Appointment Time' ASC, 'Clinic Name', 'Patient Name';
