create view v1 as

with q1 as ( select key from src where key = '5')

select * from q1;
