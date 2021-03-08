create or replace function update_hash(person_id bigint)
   returns bigint
   language plpgsql
  as
$$
begin
 update person
 set hash = md5(upper(first_name || coalesce(middle_name, '') || birth_date::text))
 where id = person_id;
 return person_id;
end;
$$;

create or replace function full_factor_person_search(p_enp text, p_snils text, p_last_name text, p_first_name text, p_hash text)
   returns json[]
   language plpgsql
  as
$$
    declare
        found_person json[];
begin
 with patients_search as (
                select id
                from core.person cp
                where cp.enp = p_enp
                    and cp.deleted is false
                union
                select id
                from core.person cp
                where cp.snils = p_snils
                    and upper(cp.first_name) = upper(p_first_name)
                    and cp.deleted is false
                union
                select id
                from core.person cp
                where cp.hash = p_hash
                    and upper(cp.last_name) = upper(p_last_name)
                    and cp.deleted is false
                ) select coalesce(array_agg(
                    json_build_object(
                        'id', patients_search.id)), '{}') into found_person from patients_search;
 return found_person;
end;
$$;