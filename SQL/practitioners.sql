-- Active: 1665389269043@@docosandynamic.c1lsds8lkx7p.ap-southeast-1.rds.amazonaws.com@3306@docosandynamic

SELECT
    clinics.name AS 'Clinic Name'
    , `admins`.display_name AS 'Staff Name'
    , IF(
        acr.role != 'owner',
        'Practice Staff',
        'Practice Manager'
        ) AS 'Type'
    , `users`.`email` AS 'Email'
    , `users`.`phone_number` AS 'Phone'
    , `users`.`created_at` AS 'Account Create Date'
    , (
        SELECT COUNT(*)
        FROM `event_logs`
        WHERE
            `event_logs`.`table_name` = 'appointments'
            AND `event_logs`.`sub_action` LIKE '%changed_status%'
            AND `event_logs`.`created_by` = `users`.`id`
            AND json_extract(
                `event_logs`.`data_changed`,
                '$.status.changes'
            ) = 'approve'
        ) AS 'Number of appointments accepted'
    , (SELECT COUNT(*)
        FROM `event_logs`
        WHERE
            `event_logs`.`table_name` = 'appointments'
            AND `event_logs`.`action` = 'created'
            AND `event_logs`.`created_by` = `users`.`id`
        ) AS 'Number of appointments added'

FROM clinics
    LEFT JOIN admin_clinic_relationship AS acr ON acr.clinic_id = clinics.id
    JOIN `admins` ON acr.admin_id = admins.id
    JOIN `users` ON `users`.`id` = `admins`.`user_id`

GROUP BY
    clinics.name
    , `Staff Name`
    , `Type`
    , acr.role
    , `Email`
    , `users`.`phone_number`
    , `Number of appointments accepted`
    , `users`.`id`


union all
SELECT
    clinics.name AS 'Clinic Name'
    , doctors.display_name AS 'Staff Name'
    , 'Practice Manager' AS 'Type'
    , `users`.`email` AS 'Email'
    , `users`.`phone_number` AS 'Phone'
    , `users`.`created_at` AS 'Account Create Date'
    , (SELECT COUNT(*)
        FROM `event_logs`
        WHERE
            `event_logs`.`table_name` = 'appointments'
            AND `event_logs`.`sub_action` LIKE '%changed_status%'
            AND `event_logs`.`created_by` = `users`.`id`
            AND json_extract(
                `event_logs`.`data_changed`,
                '$.status.changes'
            ) = 'approve'
        ) AS 'Number of appointments accepted'
    , (SELECT COUNT(*)
        FROM `event_logs`
        WHERE
            `event_logs`.`table_name` = 'appointments'
            AND `event_logs`.`action` = 'created'
            AND `event_logs`.`created_by` = `users`.`id`
        ) AS 'Number of appointments added'

FROM clinics
    JOIN doctor_clinic_relationship AS dcr ON dcr.clinic_id = clinics.id AND dcr.role = 'owner'
    JOIN `doctors` ON dcr.doctor_id = doctors.id
    JOIN `users` ON `users`.`id` = `doctors`.`user_id`

GROUP BY
    clinics.name
    , `Staff Name`
    , `Email`
    , `users`.`phone_number`
    , `users`.`id`

ORDER BY
    `Number of appointments accepted` DESC
    , `Account Create Date` DESC
    , `Clinic Name`
    , `Type`
