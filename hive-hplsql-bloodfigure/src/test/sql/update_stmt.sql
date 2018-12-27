UPDATE T t1
  SET t1.DT = 1
  WHERE t1.DT = xx
	 AND EXISTS(SELECT 1
		          FROM A t2
		          WHERE t2.id = t1.id
		            AND t2.xx = xx
	           );