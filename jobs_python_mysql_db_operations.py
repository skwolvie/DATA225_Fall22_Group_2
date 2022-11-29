from mysql.connector import connect, Error
from getpass import getpass
from mysql.connector import errorcode
import csv
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


# start point
try:
	db_connection = connect(
		host = 'localhost',
		username = 'root',
		password = getpass('enter the password for mysql server'),
		database = 'jobs_data'
	)
	print("connection object created successfully" +str(db_connection))

	cursor = db_connection.cursor()

	#Job Openings by Occupation for specified (window) timed intervals By company with job_postings > 50
	join1 = ("select d.year, o.occfamname, e.employer, count(*) as job_postings "
	"from fact_csv f inner join jobdate_csv d "
	"on f.jobdate = d.jobdate "
	"inner join occupation_csv o "
	"on f.occfam = o.occfam "
	"inner join employer_csv e "
	"on f.employerid = e.employerid "
	"where o.occfamname = \'Computer and Mathematical Occupations\' "
	"group by o.occfamname, e.employer, d.year "
	"having d.year between \'2019\' and \'2021\' "
	"and job_postings > 50 "
	"order by year, occfamname;")

	print("----------***********-------------")
	print('executing query: '+join1)
	cursor.execute(join1)
	records = cursor.fetchall()
	print('output of the query')
	for row in records:
		print(row)
	print('------ with column names ------')
	df3 = pd.read_sql(join1, db_connection)
	print(df3)


	#Job Openings by Occupation for specified (window) timed intervals: By Location

	join2 = ("select d.year, o.occfamname, l.state, count(*) as job_postings "
	"from fact_csv f inner join jobdate_csv d "
	"on f.jobdate = d.jobdate "
	"inner join occupation_csv o "
	"on f.occfam = o.occfam "
	"inner join location_csv l " 
	"on f.locid = l.locid "
	"group by o.occfamname, l.state, d.year "
	"having d.year between '2019' and '2021' "
	"order by year, occfamname;")

	print("----------***********-------------")
	print('executing query: '+join2)
	cursor.execute(join2)
	records = cursor.fetchall()
	print('output of the query')
	#for row in records:
	#	print(row)
	print('------ with column names ------')
	df4 = pd.read_sql(join2, db_connection)
	print(df4)


	#states with no of job postings for DA in descending order

	join3 = ("select l.state, r.canontitle, count(*) as job_postings "
	"from fact_csv f inner join location_csv l "
	"on f.locid = l.locid "
	"inner join jobrole_csv r "
	"on r.jobid = f.jobid "
	"group by l.state, r.canontitle "
	"having r.canontitle = \'Data Analyst\' "
	"order by job_postings desc;")

	print("----------***********-------------")
	print('executing query: '+join3)
	cursor.execute(join3)
	records = cursor.fetchall()
	print('output of the query')
	#for row in records:
	#	print(row)
	print('------ with column names ------')
	df4 = pd.read_sql(join3, db_connection)
	print(df4)


	#no of employers offering job postings in software industry

	join4 = ("select r.canontitle, count(distinct e.employer) as no_of_employers "
	"from jobrole_csv r inner join fact_csv f "
	"on r.jobid = f.jobid "
	"inner join employer_csv e "
	"on f.employerid = e.employerid "
	"inner join occupation_csv o "
	"on f.occfam = o.occfam "
	"where o.occfamname = \'Computer and Mathematical Occupations\' "
	"group by r.canontitle "
	"order by no_of_employers desc;")

	print("----------***********-------------")
	print('executing query: '+join4)
	cursor.execute(join4)
	records = cursor.fetchall()
	print('output of the query')
	#for row in records:
	#	print(row)
	print('------ with column names ------')
	df4 = pd.read_sql(join4, db_connection)
	print(df4)


	#job postings by year and occupation

	join5 = ("select d.year, o.occfamname, count(*) as job_postings "
	"from fact_csv f inner join jobdate_csv d "
	"on f.jobdate = d.jobdate "
	"inner join occupation_csv o "
	"on f.occfam = o.occfam "
	"group by d.year, o.occfamname "
	"order by occfamname, d.year;")

	print("----------***********-------------")
	print('executing query: '+join5)
	cursor.execute(join5)
	records = cursor.fetchall()
	print('output of the query')
	#for row in records:
	#	print(row)
	print('------ with column names ------')
	df4 = pd.read_sql(join5, db_connection)
	print(df4)

	cursor.close()
	db_connection.close()




except Error as e:
	print("exception occurred ")
	print(e)


finally:
	if db_connection is not None:
		db_connection.close()
		print("Database connection closed")



