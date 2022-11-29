-- SELECT datname FROM pg_database
-- create database jobs_data_225
select count(*) from employer_csv;

-- Pay Range Estimate for a Job by a firm in multiple locations. Top 10
-- Ex: SE at Amazon in (Bay, Seattle, Georgia,....) in seperate rows
select count(*) RecordsPerGroup, r.canontitle, e.employer, l.state, avg(f.minsalary) as avg_minsal, 
avg(f.maxsalary) as avg_maxsal
from fact_csv f inner join jobrole_csv r ON f.jobid = r.jobid
inner join employer_csv e on f.employerid= e.employerid
inner join location_csv l on f.locid= l.locid
where e.employer='Google Inc.' and r.canontitle= 'Software Development Engineer'
group by r.canontitle, e.employer, l.state
order by avg_maxsal desc;

select r.canontitle, e.employer, l.state, f.minsalary, f.maxsalary as avg_maxsal
from fact_csv f inner join jobrole_csv r ON f.jobid = r.jobid
inner join employer_csv e on f.employerid= e.employerid
inner join location_csv l on f.locid= l.locid
where e.employer='Google Inc.' and r.canontitle= 'Software Development Engineer' and l.state='California';


-- Pay Range Estimate for a Role.
select count(*) RecordsPerGroup, r.canontitle, avg(f.minsalary) as avg_minsal, avg(f.maxsalary) as avg_maxsal
from fact_csv f inner join jobrole_csv r ON f.jobid = r.jobid
where r.canontitle= 'Software Development Engineer'
group by r.canontitle;


-- Experience and Education Required for a role (Like) for a given pay range.
select count(*) RecordsPerGroup, r.canontitle, f.edu, max(f.maxhrlysalary) as max_maxsal,
CASE WHEN f.exp<=4 THEN '0-4'
     WHEN f.exp>4 AND f.exp<=8 THEN '4-8'
     WHEN f.exp>8 AND f.exp<=12 THEN '8-12'
     WHEN f.exp>12 THEN '12+'
     END AS expCat
from fact_csv f inner join jobrole_csv r ON f.jobid = r.jobid
where r.canontitle='Information Technology Specialist'
group by r.canontitle, f.edu, expCat
order by edu, expcat, max_maxsal;

-- Career Trajectory for a domain. (ML, Clinical Research, Store Manager)
select r.canontitle, avg(f.edu) as edu, round(avg(f.exp)) as exp, round(avg(f.maxsalary)) as maxsal
from fact_csv f inner join jobrole_csv r ON f.jobid = r.jobid
where r.canontitle like '%Clinical Research%' AND r.canontitle NOT LIKE '%Nurse%'
group by r.canontitle
order by maxsal, exp, edu;


select count(*) from fact_csv;

-- top 10 most asked skills for a given job title(Data Analyst)
select top 10 sk.skill, r.canontitle, count(sk.skill) as cnt
from fact_csv f inner join skill_csv sk
on f.jobid = sk.jobid
inner join jobrole_csv r
on f.jobid = r.jobid
inner join employer_csv e
on e.employerid = f.employerid
where r.canontitle in ('Data Analyst')
group by r.canontitle, sk.skill
order by cnt desc;

-- job postings by year
select d.year, count(*) as job_postings 
from fact_csv f inner join jobdate_csv d
on f.jobdate = d.jobdate
group by d.year;

-- job postings by year and occupation

select d.year, o.occfamname, count(*) as job_postings 
from fact_csv f inner join jobdate_csv d
on f.jobdate = d.jobdate
inner join occupation_csv o
on f.occfam = o.occfam
group by d.year, o.occfamname
order by year, occfamname;

-- job postings by quarter

select d.quarteryr, count(*) as job_postings 
from fact_csv f inner join jobdate_csv d
on f.jobdate = d.jobdate
inner join occupation_csv o
on f.occfam = o.occfam
group by d.quarteryr
order by d.quarteryr;

-- maximum postings in which quarter in each year

-- number of soc that come under each occupationFamily (soc and occ relation)

select o.occfam, o.occfamName, count(*) as soc
from soc_csv s inner join fact_csv f
on s.col0 = f.soc
inner join occupation_csv o 
on f.occfam = o.occfam
group by o.occfam, o.occfamName
order by soc desc;

-- no of socs for 'Computer and Mathematical Occupations' occupation
select o.occfamname, count(*) as soc
from soc_csv s inner join fact_csv f
on s.col0 = f.soc
inner join occupation_csv o 
on f.occfam = o.occfam
group by o.occfamname
having o.occfamname = 'Computer and Mathematical Occupations';


-- which location has highest job postings for DA
select top 1 l.state, r.canontitle, count(*) as job_postings
from fact_csv f inner join location_csv l
on f.locid = l.locid
inner join jobrole_csv r
on r.jobid = f.jobid
group by l.state, r.canontitle
having r.canontitle = 'Data Analyst'
order by job_postings desc;

-- which canon_title has highest job postings for which location
select top 1 l.state, r.canontitle, count(*) as job_postings
from fact_csv f inner join location_csv l
on f.locid = l.locid
inner join jobrole_csv r
on r.jobid = f.jobid
group by l.state, r.canontitle
order by job_postings desc;

-- state with highest job postings in each canon_title
WITH cte AS
(
   SELECT l.state, r.canontitle, count(*) as job_postings,
         ROW_NUMBER() OVER (PARTITION BY r.canontitle order by job_postings desc) AS rn
   from fact_csv f inner join location_csv l
on f.locid = l.locid
inner join jobrole_csv r
on r.jobid = f.jobid
group by r.canontitle, l.state
order by job_postings desc
)
SELECT *
FROM cte
WHERE rn = 1;


-- above query only for Computer and Mathematical Occupations
WITH cte AS
(
   SELECT l.state, r.canontitle, count(*) as job_postings,
         ROW_NUMBER() OVER (PARTITION BY r.canontitle order by job_postings desc) AS rn
   from fact_csv f inner join location_csv l
on f.locid = l.locid
inner join jobrole_csv r
on r.jobid = f.jobid
inner join occupation_csv o
on f.occfam = o.occfam
where o.occfamname = 'Computer and Mathematical Occupations'
group by r.canontitle, l.state
order by job_postings desc
)
SELECT *
FROM cte
WHERE rn = 1;
-- result: there are more job postings in california for most of the software job titles in 'Computer and Mathematical Occupations' occupation 

-- no of employers offering job postings in software industry

select r.canontitle, count(distinct e.employer) as no_of_employers
from jobrole_csv r inner join fact_csv f
on r.jobid = f.jobid
inner join employer_csv e
on f.employerid = e.employerid
inner join occupation_csv o
on f.occfam = o.occfam
where o.occfamname = 'Computer and Mathematical Occupations'
group by r.canontitle
order by no_of_employers desc;


-- How many companies have DA, ML, DS Database architect positions?

select r.canontitle, count(distinct e.employer) as no_of_employers
from jobrole_csv r inner join fact_csv f
on r.jobid = f.jobid
inner join employer_csv e
on f.employerid = e.employerid
inner join occupation_csv o
on f.occfam = o.occfam
where o.occfamname = 'Computer and Mathematical Occupations'
group by r.canontitle
having r.canontitle in ('Data Analyst','Machine Learning Engineer','Database Administrator','Database Architect')
order by no_of_employers desc;


-- Job Openings by Occupation for specified (window) timed intervals:
-- By Location

select d.year, o.occfamname, l.state, count(*) as job_postings 
from fact_csv f inner join jobdate_csv d
on f.jobdate = d.jobdate
inner join occupation_csv o
on f.occfam = o.occfam
inner join location_csv l 
on f.locid = l.locid
group by o.occfamname, l.state, d.year
having d.year between '2019' and '2021'
order by year, occfamname;

-- Job Openings by Occupation for specified (window) timed intervals:
-- By company with job_postings > 50
select d.year, o.occfamname, e.employer, count(*) as job_postings 
from fact_csv f inner join jobdate_csv d
on f.jobdate = d.jobdate
inner join occupation_csv o
on f.occfam = o.occfam
inner join employer_csv e
on f.employerid = e.employerid
where o.occfamname = 'Computer and Mathematical Occupations'
group by o.occfamname, e.employer, d.year
having d.year between '2019' and '2021'
and job_postings > 50
order by year, occfamname;