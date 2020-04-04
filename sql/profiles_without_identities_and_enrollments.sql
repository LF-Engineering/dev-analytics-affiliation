select i.* from (select p.* from profiles p left join identities i on p.uuid = i.uuid where i.uuid is null) i left join enrollments e on i.uuid = e.uuid where e.uuid is null;
