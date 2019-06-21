
CREATE TABLE device_info(
    id int primary key auto_increment,
    device VARCHAR (255),
    name VARCHAR (255)
)charset utf8;

INSERT INTO device_info (device, name) VALUES ('ido','红米手机3'),('hennessy','红米Note3'),('virgo','小米Note');

CREATE TABLE device_info_price(
    id int primary key auto_increment,
    device VARCHAR (255),
    name VARCHAR (255),
    price int
)charset utf8;

delete from device_info_price;

---------------------------------

set ido_price=20;
get ido_price;

create table device_price as
select device,price from
(SELECT 'ido' as device,
        ${ido_price} as price
UNION
SELECT 'hennessy' as device,
        30 as price
UNION
SELECT 'virgo' as device,
        40 as price
) a;

select device,price from device_price;

process repatition.`{"table":"device_price","partiton":1}`;

save json.`/Users/jaeng/Developments/OwnProject/adhesive/file/device_price` SELECT * from device_price;

create table device_price_json as json.`/Users/jaeng/Developments/OwnProject/adhesive/file/device_price`;

create table device_info as jdbc.`{"url":"jdbc:mysql://192.168.236.10:3306/miui_db_test", "userName":"root","password":"root","jdbcTable":"device_info","jdbcType":"mysql"}`;

set cache =true;

get cache;

create table device_info_price as
select device_price_json.device,
        device_info.name,
        device_price_json.price
from device_price_json,device_info
WHERE device_price_json.device = device_info.device;

get cache;

SELECT * from device_info_price;

save jdbc.`{"url":"jdbc:mysql://192.168.236.10:3306/miui_db_test", "userName":"root","password":"root","jdbcTable":"device_info_price","jdbcType":"mysql"}` SELECT * from device_info_price;

stop;

create table text_table as text.`/Users/jaeng/Developments/OwnProject/adhesive/file/text_file`;

select * from text_table;

select text_split_value(value, "\t", 0) as dt, text_split_value(value, "\t", 5) as version from text_table;

set path as select join_file_path_with_time_range("/Users/jaeng/Developments/OwnProject/adhesive/file/dt=", "20190601", "20190603");

get path;

create table text_table as text.`${path}`;
select * from text_table;
create table text_table as text.`/Users/jaeng/Developments/OwnProject/adhesive/file/dt={20190601..20190603}`;


---
create table device_price as
select device,price from
(SELECT 'ido' as device,
        20 as price
UNION
SELECT 'hennessy' as device,
        30 as price
UNION
SELECT 'virgo' as device,
        40 as price
) a
where 1=1;
process repatition.`{"table":"device_price","partiton":1}`;
save text.`/Users/jaeng/Developments/OwnProject/adhesive/file/device_price_text` SELECT * from device_price;
