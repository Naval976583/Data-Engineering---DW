MERGE INTO data AS t
USING data2 AS s
ON t.user_id = s.user_id
    AND t.rec_id = s.rec_id
    AND t.uut = s.uut
    AND t.hash_x = s.hash_x
WHEN MATCHED THEN
    UPDATE SET
        t.offer = s.offer,
        t.name = s.name,
        t.hash_y = s.hash_y,
        t.qualified = s.qualified,
        t.rules = s.rules,
WHEN NOT MATCHED THEN
    INSERT (user_id, rec_id, uut, hash_x, offer, name, hash_y, qualified, rules)
    VALUES (s.user_id, s.rec_id, s.uut, s.hash_x, s.offer, s.name, s.hash_y, s.qualified, s.rules);