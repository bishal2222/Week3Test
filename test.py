import psycopg2
import pandas as pd
from sqlalchemy import create_engine, false
from secret import secret

# using data from secret.py
host = secret.get('server')
database = secret.get('database')
user = secret.get('user')
password = secret.get('pass')

# creating engine using
con = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:5432/{database}')
conn = con.connect()

query = '''
-- create table users
Create type Roles as ENUM('client', 'driver', 'partner');
Create type Banned as ENUM('yes', 'no');
Create table If Not Exists users(users_id int primary key, banned Banned, role Roles);

-- create table trips
create type Status as ENUM('completed', 'cancelled_by_driver', 'cancelled_by_client');
Create table If Not Exists trips(id int primary key, client_id int, driver_id int, city_id int, status Status , request_at varchar(50), 
								  foreign key(client_id) references users(users_id),
								  foreign key(driver_id) references users(users_id)
								);


-- data inserted into users
insert into users values ('1', 'no', 'client'),
('2', 'yes', 'client'),
('3', 'no', 'client'),
('4', 'no', 'client'),
('10', 'no', 'driver'),
('11', 'no', 'driver'),
('12', 'no', 'driver'),
('13', 'no', 'driver');

-- data inserted into trips
insert into trips (id, client_id, driver_id, city_id, status, request_at) values ('1', '1', '10', '1', 'completed', '2013-10-01'),
 ('2', '2', '11', '1', 'cancelled_by_driver', '2013-10-01'),
 ('3', '3', '12', '6', 'completed', '2013-10-01'),
 ('4', '4', '13', '6', 'cancelled_by_client', '2013-10-01'),
('5', '1', '10', '1', 'completed', '2013-10-02'),
 ('6', '2', '11', '6', 'completed', '2013-10-02'),
('7', '3', '12', '6', 'completed', '2013-10-02'),
('8', '2', '12', '12', 'completed', '2013-10-03'),
('9', '3', '10', '12', 'completed', '2013-10-03'),
('10', '4', '13', '12', 'cancelled_by_driver', '2013-10-03');

-- total no of trips cancelled by/with unbanned user
with total_cancel_unbanned as (select  count(*) c, t.request_at
	from trips as t 
	inner join users as u
	on u.users_id = t.client_id 
	where t.status = 'cancelled_by_client' or 
		t.status = 'cancelled_by_driver' and
		u.banned = 'no' 
	group by t.request_at),

-- total req by unbanned user  
total_req_unbanned as (select count(*) r,request_at 
	from trips t
	inner join users u
	on u.users_id = t.client_id
	where u.banned = 'no'
	group by request_at)

-- calculation canceling rate
select request_at as day, coalesce(round((cancel.c::decimal/req.r),2),0) rate
	from total_req_unbanned req
	left join total_cancel_unbanned cancel
	using (request_at);


'''




df = pd.read_sql_query(query, conn)

print(df)

conn.close()