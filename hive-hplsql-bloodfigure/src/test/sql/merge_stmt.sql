MERGE INTO A
    USING B
     ON (A.id = B.id)
     WHEN NOT MATCHED THEN
     INSERT
     VALUES (B.id);