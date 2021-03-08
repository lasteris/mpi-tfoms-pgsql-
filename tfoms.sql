
create or replace function load_tfoms()
   returns int
   language plpgsql
  as
 $$
    declare
        patient record;
        found_person json[];
        found_insurance json[];
        found_attachment json[];
        found_person_chars json[];
        found_address json[];
        patients_search_length int;
        insurance_search_length int;
        attachment_search_length int;
        pc_search_length int;
        address_search_length int;
        created_p bigint;
        updated_p bigint;
        created_a bigint;
        updated_a bigint;
        created_i bigint;
        updated_i bigint;
        created_pc bigint;
        updated_pc bigint;
        created_address bigint;
        updated_address bigint;
        oms varchar(3) := 'ОМС';
        policy text;
    begin
        for patient in select * from tfoms where ignored is false order by id loop
            raise notice 'enp: %, initials: %', patient.enp, patient.last_name || ' ' || patient.first_name || ' ' || coalesce(patient.middle_name, ' ');

            select case when patient.t_policy = '3' then patient.enp else patient.n_policy end into policy;

            select full_factor_person_search(
                patient.enp,
                patient.snils,
                patient.last_name,
                patient.first_name,
                patient.hash) into found_person;

            select cardinality(found_person) into patients_search_length;

            if patients_search_length > 1 then
                raise notice 'error, too many coincidences for %', patient.id;
                update tfoms set comments = 'error, too many coincidences - ' || patients_search_length where id = patient.id;

            elsif patients_search_length = 0 then
                insert into core.person(create_date, last_name, first_name, middle_name, gender, birth_date, snils, enp, hash, comment)
                 values (current_timestamp,
                         patient.last_name,
                         patient.first_name,
                         patient.middle_name,
                         patient.gender,
                         patient.birth_date,
                         patient.snils,
                         patient.enp,
                         patient.hash,
                         'created with tfoms file at 05.02.2021'
                        )  returning id into created_p;
                raise notice 'person created with id: %', created_p;
                insert into core.insurance(create_date, person, smo, type, begin_date, end_date, category, series, number)
                values (current_timestamp,
                        created_p,
                        patient.smo,
                        patient.t_policy,
                        coalesce(patient.start_date,current_timestamp),
                        patient.actual_end_date,
                        oms,
                        patient.s_policy,
                        policy) returning id into created_i;
                raise notice 'insurance created with id: %', created_i;
                insert into core.attachment(create_date, date_in, date_out, type, mo, person, district, owner_snils)
                values (current_timestamp,
                        coalesce(patient.mo_in, current_timestamp),
                        patient.mo_out,
                        patient.method_at,
                        patient.mo,
                        created_p,
                        patient.sector_code,
                        patient.doc_snils) returning id into created_a;
                raise notice 'attachment created with id: %', created_a;
                insert into core.person_characteristics(person, attachment)
                values (created_p,
                        created_a
                       ) returning id into created_pc;
                raise notice 'pc created with id: %', created_pc;
                insert into core.address(create_date, person, aoid, plain)
                values (current_timestamp,
                        created_p,
                        patient.reg_fias,
                        patient.reg_address
                       ) returning id into created_address;
                raise notice 'address created with id: %', created_address;
            else
--------------------обновляем таблицу person--------------------
                update core.person cp
                set comment = 'previous: l: ' || cp.last_name || ' m: ' || coalesce(cp.middle_name, '') || ' f:' || cp.first_name || ' birth: ' || cp.birth_date::text || ' g:' || cp.gender || ' sn:' || coalesce(cp.snils, '') || ' enp: ' || coalesce(cp.enp, ''),
                    last_name = patient.last_name,
                    middle_name = patient.middle_name,
                    first_name = patient.first_name,
                    birth_date = patient.birth_date,
                    gender = patient.gender,
                    snils = patient.snils,
                    enp = patient.enp,
                    hash = patient.hash
                where cp.id = cast(found_person[1]->>'id' as bigint) returning cp.id into updated_p; --индексы в массиве с 1цы
                raise notice 'person updated with id: %', updated_p;
--------------------обновляем таблицу insurance--------------------
                with insurances_search as (
                    select  i.series,
                            i.number,
                            i.id
                            from core.insurance as i
                            where i.person = updated_p
                                and i.deleted is false
                ) select coalesce(array_agg(
                    json_build_object(
                        'id', insurances_search.id,
                        'series', insurances_search.series,
                        'number', insurances_search.number)), '{}') into found_insurance from insurances_search;

                select cardinality(found_insurance) into insurance_search_length;

                if insurance_search_length = 0 then
                    insert into core.insurance(create_date, person, smo, type, begin_date, end_date, category, series, number)
                    values (current_timestamp,
                            updated_p,
                            patient.smo,
                            patient.t_policy,
                            coalesce(patient.start_date,current_timestamp),
                            patient.actual_end_date,
                            oms,
                            patient.s_policy,
                            policy
                           ) returning id into created_i;
                    raise notice 'created insurance: %', created_i;
                elsif found_insurance[insurance_search_length]->>'number' <> patient.n_policy then
                    update core.insurance
                    set smo = patient.smo,
                        type = patient.t_policy,
                        end_date = patient.end_date,
                        category = oms,
                        series = patient.s_policy,
                        number = policy
                    where id = cast(found_insurance[insurance_search_length]->>'id' as bigint) returning id into updated_i;
                    raise notice 'updated most actual insurance: %', updated_i;
                end if;
--------------------обновляем таблицу attachment--------------------
                with pc_search as (
                        select id
                        from core.person_characteristics
                        where person = updated_p
                        ) select coalesce(array_agg(json_build_object(
                        'id', pc_search.id)), '{}') into found_person_chars from pc_search;

                select cardinality(found_person_chars) into pc_search_length;

                if pc_search_length = 0 then
                    insert into core.attachment(create_date, date_in, date_out, type, mo, person, district, owner_snils)
                    values (current_timestamp,
                            coalesce(patient.mo_in,current_timestamp),
                            patient.mo_out,
                            patient.method_at,
                            patient.mo,
                            updated_p,
                            patient.sector_code,
                            patient.doc_snils) returning id into created_a;
                    raise notice 'created attachment: %', created_a;
                    insert into core.person_characteristics(person, attachment)
                    values (updated_p, created_a) returning id into created_pc;
                    raise notice 'created pc: %', created_pc;
                else
                    with attachment_search as (
                        select a.id,
                               a.date_out,
                               a.date_in,
                               a.type,
                               a.district,
                               a.owner_snils
                        from core.attachment a
                        inner join core.person_characteristics as pc on a.id = pc.attachment
                        inner join core.organization as o on o.id = a.mo
                        where pc.id = cast(found_person_chars[1]->>'id' as bigint)
                        and a.deleted is false
                        and o.deleted is false
                    ) select coalesce(array_agg(
                        json_build_object(
                        'id', attachment_search.id,
                        'dateIn', attachment_search.date_in,
                        'dateOut', attachment_search.date_out,
                        'type', attachment_search.type,
                        'district', attachment_search.district,
                        'doctorSnils', attachment_search.owner_snils)), '{}') into found_attachment from attachment_search;

                    select cardinality(found_attachment) into attachment_search_length;

                    if attachment_search_length = 0 or
                        (
                           (cast(found_attachment[1]->>'doctorSnils' as text) <> patient.doc_snils) and
                            (
                               cast(found_attachment[1]->>'dateIn' as date) <> patient.mo_in or
                               cast(found_attachment[1]->>'dateOut' as date) <> patient.mo_out or
                               cast(found_attachment[1]->>'type' as integer) <> patient.method_at or
                               cast(found_attachment[1]->>'district' as text) <> patient.sector_code
                            )
                        ) then
                        insert into core.attachment (create_date, date_in, date_out, type, mo, person, district, owner_snils)
                        values (current_timestamp,
                                coalesce(patient.mo_in, current_timestamp),
                                patient.mo_out,
                                patient.method_at,
                                patient.mo,
                                updated_p,
                                patient.sector_code,
                                patient.doc_snils
                               ) returning id into created_a;
                        raise notice 'attachment added to history %', created_a;
                        update core.person_characteristics
                        set attachment = created_a
                        where id = cast(found_person_chars[1]->>'id' as bigint) returning id into updated_pc;
                    else
                        update core.attachment
                        set owner_snils = patient.doc_snils
                        where id = cast(found_attachment[1]->>'id' as bigint) returning id into updated_a;
                        raise notice 'attachment updated doctor snils %', updated_a;
                    end if;
                end if;
--------------------обновляем таблицу addresses--------------------
                with address_search as (
                    select person
                    from core.address
                    where person = updated_p
                    ) select coalesce(array_agg(
                        json_build_object(
                        'person', address_search.person)), '{}') into found_address from address_search;

                select cardinality(found_address) into address_search_length;

                if address_search_length = 0 then
                    insert into core.address(create_date, person, aoid, plain)
                    values (current_timestamp, updated_p, patient.reg_fias, patient.reg_address) returning id into created_address;
                    raise notice 'created address %', created_address;
                elsif address_search_length = 1 then
                    update core.address
                    set aoid = patient.reg_fias,
                        plain = patient.reg_address
                    where person = updated_p returning id into updated_address;
                    raise notice 'updated address %', updated_address;
                else
                    raise notice 'more than 1 address %', updated_p;
                end if;
            end if;
        end loop;
    return 0;
end $$