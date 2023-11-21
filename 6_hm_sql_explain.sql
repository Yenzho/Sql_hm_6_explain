--=============== МОДУЛЬ 6. POSTGRESQL =======================================
--= ПОМНИТЕ, ЧТО НЕОБХОДИМО УСТАНОВИТЬ ВЕРНОЕ СОЕДИНЕНИЕ И ВЫБРАТЬ СХЕМУ PUBLIC===========
SET search_path TO public;

--======== ОСНОВНАЯ ЧАСТЬ ==============

--ЗАДАНИЕ №1
--Напишите SQL-запрос, который выводит всю информацию о фильмах 
--со специальным атрибутом "Behind the Scenes".

select  *
from  film
where  special_features @> array ['Behind the Scenes'];



--ЗАДАНИЕ №2
--Напишите еще 2 варианта поиска фильмов с атрибутом "Behind the Scenes",
--используя другие функции или операторы языка SQL для поиска значения в массиве.

select  *
from  film
where  special_features && array ['Behind the Scenes'];


select *
from  film
where  'Behind the Scenes' = any(special_features); 


--ЗАДАНИЕ №3
--Для каждого покупателя посчитайте сколько он брал в аренду фильмов 
--со специальным атрибутом "Behind the Scenes.
--Обязательное условие для выполнения задания: используйте запрос из задания 1, 
--помещенный в CTE. CTE необходимо использовать для решения задания.

explain analyze -- HashAggregate  (cost=684.64..690.63 rows=599 width=12) (actual time=10.488..10.553 rows=599 loops=1)
with cte1 as (
	select  *
	from  film
	where  special_features @> array ['Behind the Scenes'])
select c.customer_id, count(c1.film_id)
from customer c 
join rental r on r.customer_id  = c.customer_id 
join inventory i on i.inventory_id = r.inventory_id 
join cte1 c1 on c1.film_id = i.film_id 
group by c.customer_id



--ЗАДАНИЕ №4
--Для каждого покупателя посчитайте сколько он брал в аренду фильмов
-- со специальным атрибутом "Behind the Scenes".
--Обязательное условие для выполнения задания: используйте запрос из задания 1,
--помещенный в подзапрос, который необходимо использовать для решения задания.
explain analyze --HashAggregate  (cost=639.36..645.35 rows=599 width=10) (actual time=8.546..8.607 rows=600 loops=1)
select r.customer_id, count(f.film_id)
from (
	select  *
	from  film
	where  special_features @> array ['Behind the Scenes']) f
left join inventory i on f.film_id = i.film_id
left join rental r on i.inventory_id = r.inventory_id
group by r.customer_id



--ЗАДАНИЕ №5
--Создайте материализованное представление с запросом из предыдущего задания
--и напишите запрос для обновления материализованного представления
create materialized view behind_scenes as
	select r.customer_id, count(f.film_id)
	from (
		select  *
		from  film
		where  special_features @> array ['Behind the Scenes']) f
	left join inventory i on f.film_id = i.film_id
	left join rental r on i.inventory_id = r.inventory_id
	group by r.customer_id

refresh materialized view behind_scenes

explain analyze --Seq Scan on behind_scenes  (cost=0.00..10.00 rows=600 width=10) (actual time=0.021..0.109 rows=600 loops=1)
select * from behind_scenes

--ЗАДАНИЕ №6
--С помощью explain analyze проведите анализ стоимости выполнения запросов из предыдущих заданий и ответьте на вопросы:
--1. с каким оператором или функцией языка SQL, используемыми при выполнении домашнего задания: 
--поиск значения в массиве затрачивает меньше ресурсов системы;

--materialized view так как мы создаем материлизованное представление которое хранится у нас на жестком диске.

--2. какой вариант вычислений затрачивает меньше ресурсов системы: 
--с использованием CTE или с использованием подзапроса.

--В моей реализации cte затрачивала чуть больше ресурсов.



--======== ДОПОЛНИТЕЛЬНАЯ ЧАСТЬ ==============

--ЗАДАНИЕ №1
--Выполняйте это задание в форме ответа на сайте Нетологии
explain analyze
select distinct cu.first_name  || ' ' || cu.last_name as name, 
	count(ren.iid) over (partition by cu.customer_id)
from customer cu
full outer join 
	(select *, r.inventory_id as iid, inv.sf_string as sfs, r.customer_id as cid
	from rental r 
	full outer join 
		(select *, unnest(f.special_features) as sf_string
		from inventory i
		full outer join film f on f.film_id = i.film_id) as inv 
		on r.inventory_id = inv.inventory_id) as ren 
	on ren.cid = cu.customer_id 
where ren.sfs like '%Behind the Scenes%'
order by count desc

--Операция hash full join достаточно затратна из за full outer join и большого количества данных,полного соединения таблиц.
--Кроме того оконная функция window agg которая забирает много ресурсов
--А также projectset развертывает масив с большим количеством строк после чего сканируется таблица и применяется функция unnest (Subquery scan on inv) которая также является ресурсоемкой 

explain analyze --HashAggregate  (cost=639.36..645.35 rows=599 width=10) (actual time=8.546..8.607 rows=600 loops=1)
select r.customer_id, count(f.film_id)
from (
	select  *
	from  film
	where  special_features @> array ['Behind the Scenes']) f
left join inventory i on f.film_id = i.film_id
left join rental r on i.inventory_id = r.inventory_id
group by r.customer_id

--1. Выполняется seq scan on film который фильтрует фильмы.
--2. Hash это хэшируется результат 
--3. seq scan on i это последовательное сканирование таблицы inventory данных которые соответствуют условиям соединения с фильмами, содержащими 'Behind the Scenes'.
--4. Hash это снова хэшируется результат 
--5. Hash Right Join идет присоединение по inventory_id
--6. Hash Right Join присоединение по film_id
--7. HashAggregate это хеш таблица для групировки записей (group by) 


--ЗАДАНИЕ №2
--Используя оконную функцию выведите для каждого сотрудника
--сведения о самой первой продаже этого сотрудника.

select f.film_id, payment_date, t.customer_id, t.staff_id, i.film_id, f.title, c.last_name as customer_last_name, c.first_name as customer_first_name
from (
	select payment_id, payment_date, customer_id, amount, staff_id, rental_id,
		first_value(payment_id) over (partition by staff_id order by payment_date)
	from payment p) t 
left join rental r on t.rental_id = r.rental_id
left join inventory i on i.inventory_id = r.inventory_id
left join film f on f.film_id = i.film_id
left join customer c on r.customer_id = c.customer_id
where payment_id = first_value 



--ЗАДАНИЕ №3
--Для каждого магазина определите и выведите одним SQL-запросом следующие аналитические показатели:
-- 1. день, в который арендовали больше всего фильмов (день в формате год-месяц-день)
-- 2. количество фильмов взятых в аренду в этот день
-- 3. день, в который продали фильмов на наименьшую сумму (день в формате год-месяц-день)
-- 4. сумму продажи в этот день

select *
from (
	select i.store_id, rental_date::date, count(*),
		row_number() over (partition by i.store_id order by count(*) desc)
	from rental r 
	join inventory i on r.inventory_id = i.inventory_id
	group by i.store_id, rental_date::date) t1 
join (
	select s.store_id, p.payment_date::date, sum(amount),
		row_number() over (partition by s.store_id order by sum(amount))
	from payment p
	join staff s on s.staff_id = p.staff_id
	group by s.store_id, p.payment_date::date) t2 on t1.store_id = t2.store_id
where t1.row_number = 1 and t2.row_number = 1



