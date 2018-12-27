insert into session.test select * from a;
insert into test select * from  session.test join b on  1=1;
insert into ETL_ERRLOG_INFO (id,time) values (1,1);

--中间表,日志表，临时表会被过滤