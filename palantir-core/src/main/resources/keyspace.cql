CREATE TABLE cluster (
group        text,
sequence     int,
cluster_name text,
uname		     text,
pword		     text,
hosts		     text,
port         int,
opscenter    text,
ops_uname    text,
ops_pword    text,
graphite     text,
graphana     text,
PRIMARY KEY (group, sequence, cluster_name)
);