
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
