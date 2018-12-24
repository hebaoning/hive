insert into t1 select * from(
    select id from a union all select id from b
    left join c on 1=1)t
    where 1=1;