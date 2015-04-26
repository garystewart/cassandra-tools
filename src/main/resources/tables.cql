DROP TABLE IF EXISTS tweets;

CREATE TABLE tweets (
  key text PRIMARY KEY,
  user_user varchar,
  text text,
  createdat timestamp,
  isLast boolean
) WITH comment='Tweets'
    AND COMPACT STORAGE
    AND read_repair_chance = 1.0;



CREATE TABLE pk_2 (
pk1 text,
pk2 text,
r1 text,
r2 text,
PRIMARY KEY ((pk1, pk2))
) ;

CREATE TABLE pk_2_ck_2 (
pk1 text,
pk2 text,
ck1 text,
ck2 text,
r1 text,
r2 text,
PRIMARY KEY ((pk1, pk2),ck1,ck2)
) ;

