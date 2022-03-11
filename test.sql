Create type Roles as ENUM('client', 'driver', 'partner');
Create type Banned as ENUM('yes', 'no');
Create table If Not Exists users(users_id int primary key, banned Banned, role Roles);

create type Status as ENUM('completed', 'cancelled_by_driver', 'cancelled_by_client');
Create table If Not Exists trips(id int primary key, client_id int, driver_id int, city_id int, status Status , request_at varchar(50), 
								  foreign key(client_id) references users(users_id),
								  foreign key(driver_id) references users(users_id)
								);


with total_cancel_unbanned as (select  count(*) c, t.request_at
	from trips as t 
	inner join users as u
	on u.users_id = t.client_id 
	where t.status = 'cancelled_by_client' or 
		t.status = 'cancelled_by_driver' and
		u.banned = 'no' 
	group by t.request_at),

total_req_unbanned as (select count(*) r,request_at 
	from trips t
	inner join users u
	on u.users_id = t.client_id
	where u.banned = 'no'
	group by request_at)

select request_at as day, coalesce(round((cancel.c::decimal/req.r),2),0) rate
	from total_req_unbanned req
	left join total_cancel_unbanned cancel
	using (request_at)
		
		
