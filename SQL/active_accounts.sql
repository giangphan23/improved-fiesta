-- Active: 1661854745457@@docosandynamic.c1lsds8lkx7p.ap-southeast-1.rds.amazonaws.com@3306@docosandynamic
SELECT
  display_name AS Name,
  d.ID AS ID,
  'Manager' AS Type_,
  CONCAT(d.ID, d.type) AS IDNew,
  DATE(u.created_at) AS `Create Date`,
  DATE(MAX(e.updated_at)) AS `Activation Date`,
  'nondoctor' AS tableName,
  IF(ISNULL(e.updated_at), 'No', 'Yes') AS `Have Activation Date`
FROM doctors d
  LEFT JOIN event_logs e
    ON d.ID = e.data_id
    AND e.action = 'activated'
    AND e.table_name = 'doctors'
  LEFT JOIN users u
    ON d.user_id = u.ID
WHERE d.status = 'active'
AND NOT FIND_IN_SET(d.ID, (SELECT
    list_user
  FROM user_exceptions
  WHERE user_exceptions.type = 'doctor'))
AND d.type = 'manager'
GROUP BY Name,
         ID,
         Type_,
         IDNew,
         tableName

-- secretary
UNION

-- secretary for clinic
SELECT DISTINCT
  a.display_name,
  a.id,
  'Secretary' AS Type_,
  CONCAT(a.id, a.type) AS IDNew,
  DATE(u.created_at) AS `Create Date`,
  DATE(ac.updated_at) `Activation Date`,
  'nondoctor' AS tableName,
  IF(ISNULL(ac.updated_at), 'No', 'Yes') AS `Have Activation Date`
FROM clinics c,
     admin_clinic_relationship ac,
     admins a
       LEFT JOIN users u
         ON a.user_id = u.id
WHERE c.status = 'active'
AND ac.status = 'active'
AND NOT FIND_IN_SET(c.id, (SELECT
    list_user
  FROM user_exceptions
  WHERE user_exceptions.type = 'clinic'))
AND c.id = ac.clinic_id
AND ac.admin_id = a.id

UNION
-- secretary for stand alone doctor
SELECT DISTINCT
  a.display_name,
  a.id,
  'Secretary' AS Type_,
  CONCAT(a.id, 'Secretary') AS IDNew,
  DATE(u.created_at) `Create Date`,
  DATE(ad.updated_at) `Activation Date`,
  'nondoctor_standalone' AS tableName,
  IF(ISNULL(ad.updated_at), 'No', 'Yes') AS `Have Activation Date`
FROM doctors d,
     admin_stadoctor_relationship ad,
     admins a
       LEFT JOIN users u
         ON a.user_id = u.id
WHERE d.status = 'active'
AND ad.status = 'active'
AND NOT FIND_IN_SET(d.id, (SELECT
    list_user
  FROM user_exceptions
  WHERE user_exceptions.type = 'doctor'))
AND d.id = ad.doctor_id
AND ad.admin_id = a.id

UNION

-- ==========================
-- doctor list
-- ==========================
SELECT
  display_name AS Name,
  d.ID AS ID,
  CASE d.type WHEN 'non_user' THEN 'Non User' WHEN 'Normal' THEN 'Staff' WHEN 'standalone' THEN 'Standalone' END AS Type_,
  CONCAT(d.ID, d.type) AS IDNew,
  DATE(u.created_at) AS `Create Date`,
  DATE(MAX(e.updated_at)) `Activation Date`,
  'doctor' AS tableName,
  IF(ISNULL(e.updated_at), 'No', 'Yes') AS `Have Activation Date`
FROM doctors d
  LEFT JOIN event_logs e
    ON d.ID = e.data_id
    AND action = 'activated'
    AND table_name = 'doctors'
  LEFT JOIN users u
    ON d.user_id = u.ID
WHERE status = 'active'
AND NOT FIND_IN_SET(d.ID, (SELECT
    list_user
  FROM user_exceptions
  WHERE type = 'doctor'))
AND d.type <> 'manager'
GROUP BY Name,
         ID,
         Type_,
         IDNew,
         tableName

UNION

-- ==========================
-- clinic list
-- ==========================
SELECT
  c.Name AS Name,
  c.ID AS ID,
  'Clinic' AS Type_,
  CONCAT(c.ID, 'clinic') AS IDNew,
  NULL AS `Create Date`,
  DATE(MAX(e.updated_at)) `Activation Date`,
  'clinic' AS tableName,
  IF(e.updated_at IS NULL, 'No', 'Yes') AS `Have Activation Date`
FROM clinics c
  LEFT JOIN event_logs e
    ON c.ID = e.data_id
    AND action = 'activated'
    AND table_name = 'clinics'
WHERE status = 'active'
AND NOT FIND_IN_SET(c.ID, (SELECT
    list_user
  FROM user_exceptions
  WHERE type = 'clinic'))
GROUP BY Name,
         ID,
         Type_,
         IDNew,
         tableName
