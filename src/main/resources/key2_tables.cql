CREATE TABLE table1 (
pk1 text,
pk2 text,
r1 text,
r2 text,
PRIMARY KEY ((pk1, pk2))
) ;

CREATE TABLE table2 (
pk1 text,
pk2 text,
ck1 text,
ck2 text,
r1 text,
r3 text,
PRIMARY KEY ((pk1, pk2),ck1,ck2)
) ;

CREATE TABLE table3  (
pk1 text,
ck1 text,
ck2 text,
r1 text,
PRIMARY KEY (pk1,ck1,ck2)
) ;
