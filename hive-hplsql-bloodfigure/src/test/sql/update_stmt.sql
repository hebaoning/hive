UPDATE DDS_CCRD_ACCT t1
  SET t1.END_DT = P_ETL_DATE
  WHERE t1.END_DT = v_max_date
	 AND EXISTS(SELECT 1
		          FROM NDS_CCRD_ACCT t2
		          WHERE t2.XACCOUNT = t1.XACCOUNT
		            AND t2.ETL_DATE = P_ETL_DATE
	           );