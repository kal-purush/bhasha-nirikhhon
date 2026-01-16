export const function_sql = {
    sql: 
    `
    --------------------------------------------------------------------------------------------Функция получения групп устройств по определенной 
    --------------------------------------------------------------------------------------------организации или для админа всех
    DROP FUNCTION IF EXISTS SelectDevs_Group_OrgId;
    CREATE OR REPLACE FUNCTION SelectDevs_Group_OrgId(
    	c_org_id VARCHAR(60)
	)
    RETURNS TABLE
    (
	    id BIGINT,
	    parent_id BIGINT, 
	    g_name VARCHAR(250), 
	    latitude VARCHAR(60), 
	    longitude VARCHAR(60),
	    org_id BIGINT, 
	    ord_num INTEGER, 
	    deleted BOOLEAN, 
	    g_info TEXT
    )
    AS $$
    	SELECT * FROM devs_groups WHERE CAST(org_id AS TEXT) LIKE c_org_id
    $$
    LANGUAGE SQL;
    --НЕОБХОДИМО УСЛОВИЯ WRITE В ДОСТУПЕ 
    --Для пользователя SELECT * FROM SelectDevs_Group_OrgId('2');
    --Для админа SELECT * FROM SelectDevs_Group_OrgId('%');



    --------------------------------------------------------------------------------------------Функция добавленя группы устройства
    DROP FUNCTION IF EXISTS AddDevs_Group;
    CREATE OR REPLACE FUNCTION AddDevs_Group(
	    c_parent_id BIGINT,
	    c_g_name VARCHAR(250), 
	    c_latitude VARCHAR(60), 
	    c_longitude VARCHAR(60), 
	    c_org_id BIGINT, 
	    c_ord_num INTEGER, 
	    c_deleted BOOLEAN, 
	    c_g_info TEXT
    ) RETURNS BIGINT
    AS $$
	    INSERT INTO devs_groups(parent_id, g_name, latitude, longitude, org_id, ord_num, deleted, g_info)
	    VALUES(c_parent_id, c_g_name, c_latitude, c_longitude, c_org_id, c_ord_num, c_deleted, c_g_info)
	    RETURNING id
    $$
    LANGUAGE SQL;

    --------------------------------------------------------------------------------------------Функция обновления группы устройства
    DROP FUNCTION IF EXISTS UpdateDevs_Group;
    CREATE OR REPLACE FUNCTION UpdateDevs_Group(
    	c_id BIGINT,
	    c_parent_id BIGINT,
        c_g_name VARCHAR(250),
        c_latitude VARCHAR(60),
        c_longitude VARCHAR(60),
        c_org_id BIGINT,
	    c_ord_num INTEGER,
        c_deleted BOOLEAN,
        c_g_info TEXT
    )
    RETURNS VOID
    as $$
    UPDATE Devs_Groups
    SET
    	parent_id = c_parent_id,
    	g_name = c_g_name,
    	latitude = c_latitude,
    	longitude = c_longitude,
    	org_id = c_org_id,
    	ord_num = c_ord_num,
    	deleted = c_deleted,
        g_info = c_g_info
    WHERE id = c_id
    $$ LANGUAGE sql;


    --------------------------------------------------------------------------------------------Функция получения данных устройств по группе 
    DROP FUNCTION IF EXISTS SelectDevs;
    CREATE OR REPLACE FUNCTION SelectDevs(
	    c_group_dev_id BIGINT
    )
    RETURNS TABLE (
    	id BIGINT,
    	group_dev_id BIGINT,
    	number VARCHAR(80), 
    	name VARCHAR(250), 
    	latitude VARCHAR(60),
    	longitude VARCHAR(60),
    	sensors JSON,
    	deleted BOOLEAN,
    	info TEXT
    )
    AS $$
    SELECT * FROM devs WHERE group_dev_id = c_group_dev_id
    $$ LANGUAGE SQL;


    --------------------------------------------------------------------------------------------Функция добавления устройства
    DROP FUNCTION IF EXISTS AddDevs;
    CREATE OR REPLACE FUNCTION AddDevs(
	    c_group_dev_id BIGINT,
	    c_number VARCHAR(80), 
	    c_name VARCHAR(250), 
	    c_latitude VARCHAR(60),
	    c_longitude VARCHAR(60),
	    c_sensors JSON,
	    c_deleted BOOLEAN,
	    c_info TEXT
    )
    RETURNS BIGINT
    AS $$
	    INSERT INTO devs (group_dev_id, number, name, latitude, longitude, sensors, deleted, info)
	    VALUES (c_group_dev_id, c_number, c_name, c_latitude, c_longitude, c_sensors, c_deleted, c_info)
	    RETURNING id
    $$ LANGUAGE SQL;


    --------------------------------------------------------------------------------------------Функция обновления устройства
    DROP FUNCTION IF EXISTS UpdateDevs;
    CREATE OR REPLACE FUNCTION UpdateDevs(
	    c_id BIGINT,
	    c_group_dev_id BIGINT,
	    c_number VARCHAR(80),
	    c_name VARCHAR(250),
	    c_latitude VARCHAR(60),
	    c_longitude VARCHAR(60),
	    c_sensors JSON,
	    c_deleted BOOLEAN,
	    c_info TEXT
    )
    RETURNS VOID 
    AS $$
    	UPDATE devs SET
    	group_dev_id=c_group_dev_id,
    	number=c_number,
    	name=c_name,
    	latitude=c_latitude,
    	longitude=c_longitude,
    	sensors=c_sensors,
    	deleted=c_deleted,
    	info = c_info 
    	WHERE id = c_id	
    $$
    LANGUAGE SQL; 


    --------------------------------------------------------------------------------------------Функция получения сессий устройства 
    DROP FUNCTION IF EXISTS SelectDev_Sess;
    CREATE OR REPLACE FUNCTION SelectDev_Sess(
    	c_dev_number VARCHAR(80),
    	start_period VARCHAR(80),
    	end_period VARCHAR(80)
    )
    RETURNS TABLE
    (
    	id BIGINT,
    	time_dev TIMESTAMP,
    	time_srv TIMESTAMP,
    	dev_number VARCHAR(80),
    	dev_id BIGINT,
    	level_akb FLOAT,
    	sess_data TEXT
    )
    AS $$ 
    SELECT * FROM dev_sess WHERE dev_number = c_dev_number AND time_dev>= CAST(start_period as TIMESTAMP) AND time_dev<=CAST(end_period as TIMESTAMP)
    $$ LANGUAGE SQL;


    --------------------------------------------------------------------------------------------Функция добавления поверочного интервала устройства
    DROP FUNCTION IF EXISTS AddDev_Povs;
    CREATE OR REPLACE FUNCTION AddDev_Povs(
	    c_dev_id BIGINT,
	    c_dev_number VARCHAR(80), 
	    c_start_povs TIMESTAMP, 
	    c_end_povs TIMESTAMP, 
	    c_old_dev_povs BIGINT
    ) RETURNS BIGINT
    AS $$
	    INSERT INTO dev_povs(dev_id, dev_number, start_povs, end_povs, old_dev_povs)
	    VALUES(c_dev_id, c_dev_number, c_start_povs, c_end_povs, c_old_dev_povs)
	    RETURNING id
    $$
    LANGUAGE SQL;

    --------------------------------------------------------------------------------------------Функция получения поверочного интервала устройства
    DROP FUNCTION IF EXISTS SelectDev_Povs;
    CREATE OR REPLACE FUNCTION SelectDev_Povs(
    	c_dev_id BIGINT,
    	c_dev_number VARCHAR(80) 
    ) RETURNS TABLE
    (
    	id BIGINT,
    	dev_id BIGINT, 
    	dev_number VARCHAR(80), 
    	start_povs TIMESTAMP, 
    	end_povs TIMESTAMP, 
    	old_dev_povs BIGINT
    )
    AS $$
    	select * from dev_povs WHERE dev_id = c_dev_id AND dev_number = c_dev_number order by id desc limit 1;
    $$
    LANGUAGE SQL;

    --------------------------------------------------------------------------------------------Функция добавления контрольной сессии 
    DROP FUNCTION IF EXISTS AddControl_Dev_Sess;
    CREATE OR REPLACE FUNCTION AddControl_Dev_Sess(
    	c_dev_sess_id BIGINT,
    	c_dev_id BIGINT,
    	c_dev_number VARCHAR(80) 
    ) RETURNS BIGINT
    AS $$
    	INSERT INTO control_dev_sess(dev_sess_id, dev_id, dev_number)
    	VALUES(c_dev_sess_id, c_dev_id, c_dev_number)
    	RETURNING id
    $$
    LANGUAGE SQL;

    --------------------------------------------------------------------------------------------Функция получения всех пользователей
    DROP FUNCTION IF EXISTS SelectAllUsers;
    CREATE OR REPLACE FUNCTION SelectAllUsers()
    RETURNS table 
    (
    	u_id BIGINT, 
        u_login VARCHAR(250), 
        u_password VARCHAR(250), 
        u_family VARCHAR(150), 
        u_name VARCHAR(150), 
        u_father VARCHAR(150), 
        u_telephone VARCHAR(50), 
        u_email VARCHAR(150), 
        u_org_id BIGINT, 
        u_job_title_id BIGINT, 
        u_roles_ids JSON,
        u_user_data JSON, 
        u_mail_code VARCHAR(250), 
        u_act_mail BOOLEAN, 
        u_re_password_code VARCHAR(250), 
        u_deleted BOOLEAN, 
        u_deleted_date TIMESTAMP, 
        u_created_at TIMESTAMP, 
        u_info TEXT,
    	jt_name VARCHAR (250), 
    	orgs_full_name VARCHAR(400)
    )
    AS $$
    SELECT users.id, users.login, users.password, users.family, users.name, users.father, users.telephone, users.email,
    users.org_id, users.job_title_id, users.roles_ids, users.user_data, users.mail_code, users.act_mail, users.re_password_code, users.deleted, 
    users.deleted_date, users.created_at, users.info, jobs_titles.name, orgs.full_name FROM users 
    JOIN jobs_titles on jobs_titles.id = users.job_title_id
    JOIN orgs on orgs.id = users.org_id
    $$
    LANGUAGE SQL;

    --------------------------------------------------------------------------------------------Функция добавления пользователя
    DROP FUNCTION IF EXISTS AddUser;
    CREATE OR REPLACE FUNCTION AddUser
    (
        c_login VARCHAR(250), 
        c_password VARCHAR(250), 
        c_family VARCHAR(150), 
        c_name VARCHAR(150), 
        c_father VARCHAR(150), 
        c_telephone VARCHAR(50), 
        c_email VARCHAR(150), 
        c_org_id BIGINT, 
        c_job_title_id BIGINT, 
        c_roles_ids JSON,
        c_user_data JSON, 
        c_mail_code VARCHAR(250), 
        c_act_mail BOOLEAN, 
        c_re_password_code VARCHAR(250), 
        c_deleted BOOLEAN, 
        c_deleted_date TIMESTAMP, 
        c_created_at TIMESTAMP, 
        c_info TEXT
    ) RETURNS BIGINT 
    AS $$
        INSERT INTO users(login, password, family, name, father, telephone, email, org_id, job_title_id, roles_ids, user_data, mail_code, act_mail, re_password_code, deleted, deleted_date, created_at, info)
        VALUES(c_login, c_password, c_family, c_name, c_father, c_telephone, c_email, c_org_id, c_job_title_id, c_roles_ids, c_user_data, c_mail_code, c_act_mail, c_re_password_code, c_deleted, c_deleted_date, c_created_at, c_info)
        RETURNING id
    $$
    LANGUAGE SQL;

    --------------------------------------------------------------------------------------------Функция обновления данных пользователя 
    DROP FUNCTION IF EXISTS UpdateUser;
    CREATE OR REPLACE FUNCTION UpdateUser(
        c_login VARCHAR(250),
        c_family VARCHAR(150),
        c_name VARCHAR(150),
        c_father VARCHAR(150),
        c_telephone VARCHAR(50),
        c_email VARCHAR(150), 
        c_info TEXT
    )
    RETURNS VOID
    as $$
    UPDATE users
    SET
        family = c_family,
        name = c_name,
        father = c_father,
        telephone = c_telephone,
        mail_code = CASE WHEN ((SELECT email FROM users WHERE login=c_login) <> c_email) THEN ('') ELSE (SELECT mail_code FROM users WHERE login=c_login) END,
        act_mail = CASE WHEN ((SELECT email FROM users WHERE login=c_login) <> c_email) THEN (false) ELSE (SELECT act_mail FROM users WHERE login=c_login) END,
        email=c_email,
        info = c_info
    WHERE login=c_login
    $$ LANGUAGE sql;

    --------------------------------------------------------------------------------------------Функция обновления данных пользователя администратором
    DROP FUNCTION IF EXISTS UpdateUserAdmin;
    CREATE OR REPLACE FUNCTION UpdateUserAdmin(
	    c_login VARCHAR(250),
	    c_password VARCHAR(250),
	    c_family VARCHAR(150),
        c_name VARCHAR(150),
        c_father VARCHAR(150),
	    c_telephone VARCHAR(50),
        c_email VARCHAR(150),
        c_id_org BIGINT,
        c_id_job BIGINT,
        c_roles_ids JSON,
	    c_mail_code VARCHAR(250),
	    c_re_password_code VARCHAR(250),
	    c_deleted BOOLEAN,
        c_info TEXT
	    )
    RETURNS VOID
    AS $$
    	UPDATE users SET
    	password = c_password,
    	family = c_family,
        name = c_name,
        father = c_father,
    	telephone = c_telephone,
        email = c_email,
        org_id = c_id_org,
        job_title_id = c_id_job,
        roles_ids = c_roles_ids,
    	mail_code = c_mail_code,
    	act_mail = CASE WHEN ((SELECT email FROM users WHERE login=c_login) <> c_email) THEN (false) ELSE (SELECT act_mail FROM users WHERE login=c_login) END,
    	re_password_code = c_re_password_code,
    	deleted = c_deleted,
    	deleted_date = CASE WHEN (c_deleted<>false) then (CURRENT_TIMESTAMP) ELSE (null) END,
        info = c_info
    	WHERE login = c_login
    $$
    LANGUAGE SQL;

    --------------------------------------------------------------------------------------------Функция обновления и подтверждения email 
    DROP FUNCTION IF EXISTS UpdateUserEmail;
    CREATE OR REPLACE FUNCTION UpdateUserEmail
    (c_mail_code VARCHAR(250),
    c_sess_code VARCHAR(250))
    RETURNS VOID AS $$
        UPDATE users 
        SET mail_code = c_mail_code,
        act_mail = true
        WHERE login = (select login from users inner join sessions on sessions.uid = users.id where sess_code = c_sess_code)
    $$ LANGUAGE sql;

    --------------------------------------------------------------------------------------------Функция обновления пароля пользователем
    DROP FUNCTION IF EXISTS ChangePass;
    CREATE OR REPLACE FUNCTION ChangePass(
        c_login VARCHAR(250),
        c_new_password VARCHAR(250)
    )
    RETURNS VOID
    as $$
    UPDATE users
    SET
        password = c_new_password
    	WHERE login=c_login;
    $$ LANGUAGE sql;

    --------------------------------------------------------------------------------------------Функция создания/обновления кода для восстановления пароля 
    DROP FUNCTION IF EXISTS UpdateRePassCode;
    CREATE OR REPLACE FUNCTION UpdateRePassCode(
        c_login VARCHAR(250),
        c_re_password_code VARCHAR(250))
    RETURNS VOID AS $$
        UPDATE users
        SET re_password_code = c_re_password_code
        WHERE login = c_login
    $$ LANGUAGE sql;


    --------------------------------------------------------------------------------------------Функция забыли пароль 
    DROP FUNCTION IF EXISTS ForgPass;
    create or replace function ForgPass(
    	c_login VARCHAR(250),
    	c_password VARCHAR(250),
    	c_re_password_code VARCHAR(250))
    RETURNS VOID AS $$
    	UPDATE users
    	SET re_password_code = c_re_password_code,
    	password = c_password
    	WHERE login = c_login
    $$ LANGUAGE sql; 

    --------------------------------------------------------------------------------------------Функция добавления сессии при авторизации
    DROP FUNCTION IF EXISTS AddUserSession;
    CREATE OR REPLACE FUNCTION AddUserSession 
    (
    c_Uid BIGINT,
    c_Expires TIMESTAMP,
    c_Created_at TIMESTAMP,
    c_Sess_code VARCHAR(250),
    c_Sess_data JSON
    ) RETURNS VOID AS $$
    BEGIN 
        INSERT INTO sessions(uid, expires, created_at, sess_code, sess_data)
        VALUES(c_Uid, c_Expires, c_Created_at,c_Sess_code, c_Sess_data);
    END
    $$
    LANGUAGE 'plpgsql';

    --------------------------------------------------------------------------------------------Функция удаления сессии при выходе из программы
    DROP FUNCTION IF EXISTS DeleteSessions;
    CREATE OR REPLACE FUNCTION DeleteSessions (
        c_sess_code VARCHAR(250)
    ) RETURNS VOID AS $$
        DELETE FROM sessions WHERE sess_code=c_sess_code
    $$ 
    LANGUAGE sql;

    --------------------------------------------------------------------------------------------Функция получения данных при авторизации по логину и паролю
    DROP FUNCTION IF EXISTS SelectUser;
    CREATE OR REPLACE FUNCTION SelectUser(
        c_login VARCHAR(250), 
        c_password VARCHAR(250)
    )
    RETURNS table(
        id BIGINT, 
        login VARCHAR(250), 
        password VARCHAR(250), 
        family VARCHAR(150), 
        name VARCHAR(150), 
        father VARCHAR(150), 
        telephone VARCHAR(50), 
        email VARCHAR(150), 
        org_id BIGINT, 
        job_title_id BIGINT, 
        roles_ids JSON,
        user_data JSON, 
        mail_code VARCHAR(250), 
        act_mail BOOLEAN, 
        re_password_code VARCHAR(250), 
        deleted BOOLEAN, 
        deleted_date TIMESTAMP, 
        created_at TIMESTAMP, 
        info TEXT
    )
    as $$
        SELECT users
        FROM users 
        WHERE login = c_login and password = c_password
    $$ LANGUAGE sql;

    --------------------------------------------------------------------------------------------Функция получения данных по email
    DROP FUNCTION IF EXISTS SelectUserLoginEmail;
    CREATE OR REPLACE FUNCTION SelectUserLoginEmail(
        c_data VARCHAR(250)
    )
    RETURNS table(
        password VARCHAR(250),
        act_mail BOOLEAN, 
        re_password_code VARCHAR(250) 
    )
    as $$
        SELECT password, act_mail, re_password_code
        FROM users 
        WHERE login = c_data OR email = c_data
    $$ LANGUAGE sql;
    --------------------------------------------------------------------------------------------Функция получения данных по коду сессии
    DROP FUNCTION IF EXISTS SelectUserBySessCode;
    CREATE OR REPLACE FUNCTION SelectUserBySessCode(
        c_sess_code VARCHAR(250)
    )
    RETURNS table(
        id BIGINT, 
        login VARCHAR(250), 
        password VARCHAR(250), 
        family VARCHAR(150), 
        name VARCHAR(150), 
        father VARCHAR(150), 
        telephone VARCHAR(50), 
        email VARCHAR(150), 
        org_id BIGINT, 
        job_title_id BIGINT, 
        roles_ids JSON,
        user_data JSON, 
        mail_code VARCHAR(250), 
        act_mail BOOLEAN, 
        re_password_code VARCHAR(250), 
        deleted BOOLEAN, 
        deleted_date TIMESTAMP, 
        created_at TIMESTAMP, 
        info TEXT
    )
    as $$
        SELECT users
        FROM users 
        INNER JOIN sessions on users.id = sessions.uid
        WHERE sessions.sess_code = c_sess_code
    $$ LANGUAGE sql;

    --------------------------------------------------------------------------------------------Функция получения данных сессии по коду
    DROP FUNCTION IF EXISTS SelectSessCode;
    CREATE OR REPLACE FUNCTION SelectSessCode(
        c_sess_code VARCHAR(250)
    )
    RETURNS table(
        id BIGINT, 
        uid BIGINT, 
        expires TIMESTAMP, 
        created_at TIMESTAMP,
        sess_code VARCHAR(250), 
        sess_data JSON
    )
    as $$
        SELECT 
        sessions
        FROM sessions 
        WHERE sess_code = c_sess_code
    $$ LANGUAGE sql;

    --------------------------------------------------------------------------------------------Функция добавления организации
    DROP FUNCTION IF EXISTS AddOrgs;
    CREATE OR REPLACE FUNCTION AddOrgs(
    	c_name VARCHAR(250),
    	c_full_name VARCHAR(400),
    	c_inn VARCHAR(50),
    	c_address VARCHAR(400),
    	c_latitude VARCHAR(60),
    	c_longitude VARCHAR(60),
    	c_created_at TIMESTAMP,
    	c_info TEXT)
    RETURNS BIGINT
    AS $$
    	INSERT INTO orgs (name, full_name, inn, address, latitude, longitude, created_at, info)
    	VALUES (c_name, c_full_name, c_inn, c_address, c_latitude, c_longitude, c_created_at, c_info)
    	RETURNING id
    $$
    LANGUAGE SQL;


    --------------------------------------------------------------------------------------------Функция получения всех организаций
    DROP FUNCTION IF EXISTS SelectOrgs;
    CREATE OR REPLACE FUNCTION SelectOrgs()
    RETURNS TABLE (
    	id BIGINT, 
    	name VARCHAR(250),
    	full_name VARCHAR(400),
    	inn VARCHAR(50),
    	address VARCHAR(400),
	    latitude VARCHAR(60),
	    longitude VARCHAR(60),
	    created_at TIMESTAMP,
	    info TEXT
    )
    AS $$
    SELECT * FROM orgs
    $$
    LANGUAGE SQL;

    --------------------------------------------------------------------------------------------Функция обновления организации
    DROP FUNCTION IF EXISTS UpdateOrgs;
    CREATE OR REPLACE FUNCTION UpdateOrgs(
    	c_id BIGINT,
    	c_name VARCHAR(250),
    	c_full_name VARCHAR(400),
    	c_inn VARCHAR(50),
    	c_address VARCHAR(400),
    	c_latitude VARCHAR(60),
    	c_longitude VARCHAR(60),
    	c_info TEXT
    )
    RETURNS VOID 
    AS $$
    	UPDATE orgs SET
    	id = c_id, 
    	name = c_name,
    	full_name = c_full_name,
    	inn = c_inn,
    	address = c_address,
    	latitude = c_latitude,
    	longitude = c_longitude,
    	info = c_info
    	WHERE id = c_id
    $$
    LANGUAGE SQL; 

    --------------------------------------------------------------------------------------------Функция получения должностей организации
    DROP FUNCTION IF EXISTS SelectJobs_titles;
    CREATE OR REPLACE FUNCTION SelectJobs_titles(
    	c_org_id BIGINT
    )
    RETURNS table (
    	id BIGINT,
    	org_id BIGINT,
    	name VARCHAR(250),
    	created_at TIMESTAMP,
    	info TEXT
    )AS $$
    	SELECT * FROM jobs_titles WHERE org_id = c_org_id
    $$ 
    LANGUAGE SQL;

    --------------------------------------------------------------------------------------------Функция добавления должности
    DROP FUNCTION IF EXISTS addJobs_titles;
    CREATE OR REPLACE FUNCTION addJobs_titles(
    	c_org_id BIGINT,
    	c_name VARCHAR(250),
    	c_created_at TIMESTAMP,
    	c_info TEXT
    )
    RETURNS BIGINT
    AS $$
    	INSERT INTO jobs_titles (org_id, name, created_at, info)
    	VALUES (c_org_id, c_name, c_created_at, c_info)
    	RETURNING id
    $$ 
    LANGUAGE SQL;

    --------------------------------------------------------------------------------------------Функция обновления должности
    DROP FUNCTION IF EXISTS UpdateJobs_titles;
    CREATE OR REPLACE FUNCTION UpdateJobs_titles(
    	c_id BIGINT,
    	c_org_id BIGINT,
    	c_name VARCHAR(250),
    	c_info TEXT
    )
    RETURNS VOID 
    AS $$
    	UPDATE jobs_titles SET
    	id = c_id, 
    	org_id = c_org_id,
    	name = c_name,
    	info = c_info
    	WHERE id = c_id
    $$
    LANGUAGE SQL; 
    `,
    args: new Array()
};