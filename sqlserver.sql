With temp (id,path,rem) AS
(
select 0 as id,CONVERT(varchar(1000),'') as path , CONVERT(varchar(1000),name) as rem from test where id=1
union all
select id+1,
CONVERT(varchar(1000),CONCAT(path,(case when SUBSTRING(rem,1,1) in ('1','2','3') then substring(rem,1,1) else '' end))),
CONVERT(varchar(1000),substring(rem,2,10)) as rem
from temp
where len(rem)>0
)
select * from temp

