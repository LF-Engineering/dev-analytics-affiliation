insert into access_control_entries(scope, subject, resource, action, effect) select distinct slug, 'lgryglicki', 'identity', 'manage', 0 from projects;
insert into access_control_entries(scope, subject, resource, action, effect) select distinct '/projects/' || slug, 'lgryglicki', 'identity', 'manage', 0 from projects;
