MERGE INTO atest  A
    USING  ( SELECT * FROM btest B where a.id = b.id ) c ON  A.id = c.id
      WHEN  MATCHED  THEN   UPDATE
            SET a.age = b.age ;