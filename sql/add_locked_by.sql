-- Adds `locked_by` column to DB tables
alter table enrollments add locked_by varchar(128);
alter table identities add locked_by varchar(128);
alter table profiles add locked_by varchar(128);
alter table uidentities add locked_by varchar(128);
