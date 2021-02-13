--после импорта переименуем файл, чтобы в скрипте ничего не менять
--alter table tfoms_file_name rename to tfoms;

--неподготовленная структура (исключительно для импорта, далее поправим)
create table tfoms
(
    	id bigserial not null
		constraint tfoms_pk
			primary key,
	last_name text,
	first_name text,
	middle_name text,
	gender text,
	birth_date text,
	snils text,
	enp text,
	reg_address text,
	reg_fias text,
	smo_code text,
	smo_ogrn text,
	start_date text,
	actual_end_date text,
	end_date text,
	t_policy text,
	s_policy text,
	n_policy text,
	termination_reason text,
	mo_code text,
	mo_in text,
	mo_out text,
	sector_code text,
	doc_snils text,
	method_at text,
	comments text
);

alter table tfoms owner to mpi;

---первая транзакция приводит 'пустые' значения к адекватно-пустым, облегчаем работу будущим индексам
begin transaction;
update tfoms
set last_name = null
where last_name = ''
or last_name = 'NULL';

update tfoms
set first_name = null
where first_name = ''
or first_name = 'NULL';

update tfoms
set middle_name = null
where middle_name = ''
or middle_name = 'NULL';--6037

update tfoms
set gender = null
where gender = ''
or gender = 'NULL';

update tfoms
set birth_date = null
where birth_date = ''
or birth_date = 'NULL';

update tfoms
set snils = null
where snils = ''
or snils = 'NULL';--264342

update tfoms
set enp = null
where enp = ''
or enp = 'NULL';--пусто

update tfoms
set reg_address = null
where reg_address = ''
or reg_address = 'NULL';

update tfoms
set reg_fias = null
where reg_fias = ''
or reg_fias = 'NULL';--1672779

update tfoms
set smo_code = null
where smo_code = ''
or smo_code = 'NULL';

update tfoms
set smo_ogrn = null
where smo_ogrn = ''
or smo_ogrn = 'NULL';

update tfoms
set start_date = null
where start_date = ''
or start_date = 'NULL';

update tfoms
set actual_end_date = null
where actual_end_date = ''
or actual_end_date = 'NULL';--2222340

update tfoms
set end_date = null
where end_date = ''
or end_date = 'NULL';--2238477

update tfoms
set t_policy = null
where t_policy = ''
or t_policy = 'NULL';--7

update tfoms
set s_policy = null
where s_policy = ''
or s_policy = 'NULL';--2209551

update tfoms
set n_policy = null
where n_policy = ''
or n_policy = 'NULL';--2

update tfoms
set termination_reason = null
where termination_reason = ''
or termination_reason = 'NULL';--2236748

update tfoms
set mo_code = null
where mo_code = ''
or mo_code = 'NULL';

update tfoms
set mo_in = null
where mo_in = ''
or mo_in = 'NULL';--2083

update tfoms
set mo_out = null
where mo_out = ''
or mo_out = 'NULL';--2240721

update tfoms
set sector_code = null
where sector_code = ''
or sector_code = 'NULL';--57873

update tfoms
set doc_snils = null
where doc_snils = ''
or doc_snils = 'NULL';--59030

update tfoms
set method_at = null
where method_at = ''
or method_at = 'NULL';--857
commit transaction;

--изначально сделали импорт всех полей как строки, так что начинаем конверсии
--сперва даты - в файле они все в формате dd.MM.yyyy
begin;
alter table tfoms alter column birth_date type date using to_date(birth_date, 'dd.MM.yyyy');
alter table tfoms alter column start_date type date using to_date(start_date, 'dd.MM.yyyy');
alter table tfoms alter column actual_end_date type date using to_date(actual_end_date, 'dd.MM.yyyy');
alter table tfoms alter column end_date type date using to_date(end_date, 'dd.MM.yyyy');
alter table tfoms alter column mo_in type date using to_date(mo_in, 'dd.MM.yyyy');
alter table tfoms alter column mo_out type date using to_date(mo_out, 'dd.MM.yyyy');
commit;

--также в таблице attachment: поле type - integer
alter table tfoms alter column method_at type integer using cast(method_at as integer);
--в файле есть нулл-значения
update tfoms set method_at = 0 where method_at is null;

--может попадаться английская M вместо русской => меняем
do $$
begin
    if exists(select * from tfoms where gender = 'M') then
        update tfoms set gender = 'М' where gender = 'M';
    end if;
end $$;

--подгоняем тип полиса. 1 - старый, 2 - временный, 3 - енп
begin;
update tfoms set t_policy = '3' where t_policy in ('Э', 'П'); --2193496
update tfoms set t_policy = '2' where t_policy = 'В';--16058
update tfoms set t_policy = '1' where t_policy = 'С';--31160
commit;

alter table tfoms alter column t_policy type varchar(1);

--Приводим ФИО в нормальный вид
begin;
update tfoms set last_name = initcap(last_name);
update tfoms set first_name = initcap(first_name);
update tfoms set middle_name = initcap(middle_name) where middle_name is not null;
update tfoms set middle_name = null where middle_name = '-';
commit;

--строим хеш, поможет ускорить работу транзакции
alter table tfoms add hash text;
update tfoms set hash = md5(upper(first_name || coalesce(middle_name, '') || birth_date::text));

--функциональные индексы и не только: upper, потому что сравнение
-- в update на всякий обе стороны из tfoms и person буду приводить к upper/lower?
begin;
create index idx_last on tfoms using btree(upper(last_name));
create index idx_first on tfoms using btree(upper(first_name));
create index idx_middle on tfoms using btree(upper(middle_name));
create index idx_hash on tfoms (hash);
commit;

--создадим ссылки на smo и mo, чтобы не выполнять в select поиск по кодам.
alter table tfoms add mo bigint;
alter table tfoms add smo bigint;

begin;
update tfoms set smo = o.id
from core.organization o
where o.code_foms = tfoms.smo_code;--2,240,721

update tfoms set mo = o.id
from core.organization o
where o.code_foms = tfoms.mo_code;--2,240,597
commit;

alter table tfoms add ignored boolean default false;

--проверить записи, у которых first_name - null
update tfoms set ignored = true where first_name is null;--1
--проверить, что нету записей, у которых mo - null
update tfoms set ignored = true where  mo is null;--124
--проверить, что нет записей, у которых type (тип полиса) - null
update tfoms set ignored = true where t_policy is null;--7

do $$
<<patient_block>>
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
        for patient in select * from tfoms where ignored is false limit 100 loop
            raise notice 'enp: %, initials: %', patient.enp, patient.last_name || ' ' || patient.first_name || ' ' || coalesce(patient.middle_name, ' ');

            select case when patient.t_policy = '3' then patient.enp else patient.n_policy end into policy;

            with patients_search as (
                select id
                from core.person cp
                where cp.enp = patient.enp
                    and cp.enp is not null
                    and cp.deleted is false
                union
                select id
                from core.person cp
                where cp.snils = patient.snils
                    and upper(cp.first_name) = upper(patient.first_name)
                    and cp.birth_date = patient.birth_date
                    and cp.snils is not null
                    and cp.deleted is false
                union
                select id
                from core.person cp
                where cp.hash = patient.hash
                    and upper(cp.last_name) = upper(patient.last_name)
                    and cp.deleted is false
                ) select coalesce(array_agg(
                    json_build_object(
                        'id', patients_search.id)), '{}') into found_person from patients_search;

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
                            --inner join core.organization as o on o.id = i.smo
                            where i.person = updated_p
                                and i.deleted is false
                                --and o.deleted is false
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
                else
                    update core.address
                    set aoid = patient.reg_fias,
                        plain = patient.reg_address
                    where person = cast(found_address[1]->>'person' as bigint) returning id into updated_address;
                    raise notice 'updated address %', updated_address;
                end if;
            end if;
        end loop;
end patient_block $$;

--немного статистики
select enp, count(enp) from core.person where deleted is false group by enp having count(enp) > 1;--4956
select snils, count(snils) from core.person where deleted is false group by snils having count(snils) > 1;--8927
select person, count(person) from core.address where deleted is false group by person having count(person) > 1;

select count(*) from tfoms;