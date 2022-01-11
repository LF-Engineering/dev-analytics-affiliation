-- Adds `last_modified_by` column to DB tables
alter table domains_organizations add last_modified_by varchar(128);
alter table enrollments add last_modified_by varchar(128);
alter table enrollments_archive add last_modified_by varchar(128);
alter table identities add last_modified_by varchar(128);
alter table identities_archive add last_modified_by varchar(128);
alter table matching_blacklist add last_modified_by varchar(128);
alter table organizations add last_modified_by varchar(128);
alter table profiles add last_modified_by varchar(128);
alter table profiles_archive add last_modified_by varchar(128);
alter table slug_mapping add last_modified_by varchar(128);
alter table uidentities add last_modified_by varchar(128);
alter table uidentities_archive add last_modified_by varchar(128);
