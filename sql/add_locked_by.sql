-- Adds `locked_by` column to DB tables
alter table enrollments add locked_by varchar(128);
alter table identities add locked_by varchar(128);
alter table profiles add locked_by varchar(128);
alter table uidentities add locked_by varchar(128);
-- Indices
create index enrollments_locked_by_idx on enrollments(locked_by);
create index identities_locked_by_idx on identities(locked_by);
create index profiles_locked_by_idx on profiles(locked_by);
create index uidentities_locked_by_idx on uidentities(locked_by);
