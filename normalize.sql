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