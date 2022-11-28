
SELECT
    c.id as clinic_id,
    c.name as clinic_name,
    s.name_vi as 'primary_spec_name_vi',
    s.name_en as 'primary_spec_name_en'


FROM clinics c
    LEFT JOIN clinic_specialty cs ON c.id = cs.clinic_id
    LEFT JOIN specialty s ON cs.specialty_id = s.id

WHERE
    is_primary LIKE 1 AND
    NOT FIND_IN_SET(c.id, (SELECT
        list_user
    FROM user_exceptions
    WHERE type = 'clinic'))

;
