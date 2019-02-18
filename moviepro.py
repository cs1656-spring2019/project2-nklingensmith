import sqlite3 as lite
import csv
import re
import os
con = lite.connect('cs1656.sqlite')

with con:
	cur = con.cursor() 

	########################################################################		
	### CREATE TABLES ######################################################
	########################################################################		
	# DO NOT MODIFY - START 
	cur.execute('DROP TABLE IF EXISTS Actors')
	cur.execute("CREATE TABLE Actors(aid INT, fname TEXT, lname TEXT, gender CHAR(6), PRIMARY KEY(aid))")

	cur.execute('DROP TABLE IF EXISTS Movies')
	cur.execute("CREATE TABLE Movies(mid INT, title TEXT, year INT, rank REAL, PRIMARY KEY(mid))")

	cur.execute('DROP TABLE IF EXISTS Directors')
	cur.execute("CREATE TABLE Directors(did INT, fname TEXT, lname TEXT, PRIMARY KEY(did))")

	cur.execute('DROP TABLE IF EXISTS Cast')
	cur.execute("CREATE TABLE Cast(aid INT, mid INT, role TEXT)")

	cur.execute('DROP TABLE IF EXISTS Movie_Director')
	cur.execute("CREATE TABLE Movie_Director(did INT, mid INT)")
	# DO NOT MODIFY - END

	########################################################################		
	### READ DATA FROM FILES ###############################################
	########################################################################		
	# actors.csv, cast.csv, directors.csv, movie_dir.csv, movies.csv
	# UPDATE THIS
	with open('actors.csv') as actorcsvfile:
		actorreader = csv.reader(actorcsvfile, delimiter=',')
		for row in actorreader:
			cur.execute(f"INSERT INTO Actors VALUES({int(row[0])}, '{row[1]}', '{row[2]}', '{row[3]}')")

	with open('movies.csv') as moviescsvfile:
		moviesreader = csv.reader(moviescsvfile, delimiter=',')
		for row in moviesreader:
			cur.execute(f"INSERT INTO Movies VALUES({int(row[0])}, '{row[1]}', {int(row[2])}, {float(row[3])})")

	with open('cast.csv') as castcsvfile:
		castreader = csv.reader(castcsvfile, delimiter=',')
		for row in castreader:
			cur.execute(f"INSERT INTO Cast VALUES({int(row[0])}, '{int(row[1])}', '{row[2]}')")

	with open('directors.csv') as directorcsvfile:
		directorreader = csv.reader(directorcsvfile, delimiter=',')
		for row in directorreader:
			cur.execute(f"INSERT INTO Directors VALUES({int(row[0])}, '{row[1]}', '{row[2]}')")

	with open('movie_dir.csv') as movdircsvfile:
		movdirreader = csv.reader(movdircsvfile, delimiter=',')
		for row in movdirreader:
			cur.execute(f"INSERT INTO Movie_Director VALUES({int(row[0])}, {int(row[1])})")

	




	########################################################################		
	### INSERT DATA INTO DATABASE ##########################################
	########################################################################		
	# UPDATE THIS TO WORK WITH DATA READ IN FROM CSV FILES
	#cur.execute("INSERT INTO Actors VALUES(1001, 'Harrison', 'Ford', 'Male')") 
	#cur.execute("INSERT INTO Actors VALUES(1002, 'Daisy', 'Ridley', 'Female')")   
	
	#cur.execute("INSERT INTO Movies VALUES(101, 'Star Wars VII: The Force Awakens', 2015, 8.2)") 
	#cur.execute("INSERT INTO Movies VALUES(102, 'Rogue One: A Star Wars Story', 2016, 8.0)")
	
	#cur.execute("INSERT INTO Cast VALUES(1001, 101, 'Han Solo')")  
	#cur.execute("INSERT INTO Cast VALUES(1002, 101, 'Rey')")  

	#cur.execute("INSERT INTO Directors VALUES(5000, 'J.J.', 'Abrams')")  
	
	#cur.execute("INSERT INTO Movie_Director VALUES(5000, 101)")  

	con.commit()
    
    	

	########################################################################		
	### QUERY SECTION ######################################################
	########################################################################		
	queries = {}

	# DO NOT MODIFY - START 	
	# DEBUG: all_movies ########################
	queries['all_movies'] = '''
SELECT * FROM Movies
'''	
	# DEBUG: all_actors ########################
	queries['all_actors'] = '''
SELECT * FROM Actors
'''	
	# DEBUG: all_cast ########################
	queries['all_cast'] = '''
SELECT * FROM Cast
'''	
	# DEBUG: all_directors ########################
	queries['all_directors'] = '''
SELECT * FROM Directors
'''	
	# DEBUG: all_movie_dir ########################
	queries['all_movie_dir'] = '''
SELECT * FROM Movie_Director
'''	
	# DO NOT MODIFY - END

	########################################################################		
	### INSERT YOUR QUERIES HERE ###########################################
	########################################################################		
	# NOTE: You are allowed to also include other queries here (e.g., 
	# for creating views), that will be executed in alphabetical order.
	# We will grade your program based on the output files q01.csv, 
	# q02.csv, ..., q12.csv

	# Q01 ########################		
	queries['q01'] = '''
	SELECT fname, lname 
	FROM Actors a, Cast c1, Cast c2, Movies m1, Movies m2
	WHERE a.aid = c1.aid
	AND c1.mid = m1.mid
	AND (m1.year >= 1980 AND m1.year <= 1990)
	AND a.aid = c2.aid
	AND c2.mid = m2.mid
	AND m2.year >= 2000
	ORDER BY lname ASC, fname ASC
'''	
	
	# Q02 ########################		
	queries['q02'] = '''
	SELECT title, year
	FROM Movies
	WHERE year = (SELECT year FROM Movies where title = 'Rogue One: A Star Wars Story')
	AND rank > (SELECT rank FROM Movies where title = 'Rogue One: A Star Wars Story')
	ORDER BY title ASC
'''	

	# Q03 ########################
	
	queries['a'] = '''
	DROP VIEW IF EXISTS movie_count
	'''

	queries['b'] = '''
	create view movie_count as
		SELECT DISTINCT c.aid, count(role) as num
		FROM Movies m, Cast c
		WHERE m.mid = c.mid
		AND m.title LIKE '%Star Wars%'
		GROUP BY c.aid
		HAVING num >= 1
	'''

	queries['q03'] = '''
	SELECT mc.num, a.fname, a.lname
	FROM Actors a, movie_count mc
	WHERE a.aid = mc.aid
	ORDER BY mc.num DESC, a.lname ASC, a.fname ASC
'''	

	# Q04 ########################		
	queries['q04'] = '''
	SELECT a.fname, a.lname
	FROM Actors a, Cast c, Movies m
	WHERE a.aid = c.aid
	AND c.mid = m.mid
	AND m.year < 1985
	ORDER BY lname ASC, fname ASC
'''	

	# Q05 ########################		

	queries['c'] = '''
	DROP VIEW IF EXISTS dir_count
	'''

	queries['d'] = '''
	create view dir_count as
		SELECT DISTINCT m.did, count(m.mid) as num
		FROM Movie_Director m
		GROUP BY m.did
		HAVING num >= 1
	'''

	queries['q05'] = '''
	SELECT fname, lname, num
	FROM Directors d, dir_count dc
	WHERE d.did = dc.did
	ORDER BY num DESC
	LIMIT 20
'''	

	# Q06 ########################

	queries['e'] = '''
	DROP VIEW IF EXISTS movcast_count
	'''

	queries['f'] = '''
	create view movcast_count as
		SELECT DISTINCT m.mid, count(c.aid) as num
		FROM Cast c, Movies m
		WHERE c.mid = m.mid
		GROUP BY title
		HAVING num >= 1
	'''

	queries['q06'] = '''
	SELECT title, num
	FROM Movies m, movcast_count mc
	WHERE m.mid = mc.mid
	AND mc.num >= (SELECT min(num) from (SELECT num from movcast_count ORDER BY num DESC LIMIT 10))
	ORDER BY num DESC

'''	

	# Q07 ########################

	queries['g'] = '''
	DROP VIEW IF EXISTS fem_count
	'''

	queries['h'] = '''
	create view fem_count as
		SELECT DISTINCT c.mid, sum(CASE WHEN a.gender = 'Female' THEN 1 ELSE 0 END) as fnum, sum(CASE WHEN a.gender = 'Male' THEN 1 ELSE 0 END) as mnum
		FROM Cast c, Actors a
		WHERE c.aid = a.aid
		GROUP BY c.mid
	'''

	queries['q07'] = '''
	SELECT title, fnum, mnum
	FROM Movies m, fem_count fc
	WHERE m.mid = fc.mid
	AND fc.fnum > fc.mnum
	ORDER BY title ASC
'''	

	# Q08 ########################

	

	queries['q08'] = '''
	SELECT fname, lname, dcount
	FROM (SELECT DISTINCT c.aid, a.fname, a.lname, count(*) as dcount
		FROM Cast c, Actors a, Movie_Director md, Directors d
		WHERE c.mid = md.mid
		AND c.aid = a.aid
		AND d.did = md.did
		AND NOT ( a.fname = d.fname AND a.lname = d.lname)
		GROUP BY c.aid
		HAVING dcount >= 1)
	ORDER BY dcount DESC
'''	

	# Q09 ########################		
	
	queries['i'] = '''
	DROP VIEW IF EXISTS debut_count
	'''

	queries['j'] = '''
	create view debut_count as
		SELECT MIN(year) as mini, fname, lname, aid, m.mid
		FROM Actors a, Cast c, Movies m
		WHERE a.aid = c.aid
		AND m.mid = c.mid
		GROUP BY a.aid
	'''
	
	queries['q09'] = '''
	SELECT fname, lname, COUNT(*)
	FROM Actors a, Cast c, Movies m
	WHERE a.aid = c.aid
	AND m.mid = c.mid
	AND m.year = (SELECT mini from debut_count dc WHERE aid = a.aid)
	GROUP BY fname, lname
	ORDER BY COUNT(*) DESC

'''	

	# Q10 ########################		
	queries['q10'] = '''
'''	

	# Q11 ########################		
	queries['q11'] = '''
'''	

	# Q12 ########################		
	queries['q12'] = '''
	SELECT fname, lname, COUNT(*) as ct, AVG(rank) as avgrk
	FROM Actors a, Cast c, Movies m
	WHERE a.aid = c.aid
	AND c.mid = m.mid
	GROUP BY fname, lname
	ORDER BY avgrk DESC
	LIMIT 20
'''	


	########################################################################		
	### SAVE RESULTS TO FILES ##############################################
	########################################################################		
	# DO NOT MODIFY - START 	
	for (qkey, qstring) in sorted(queries.items()):
		try:
			cur.execute(qstring)
			all_rows = cur.fetchall()
			
			print ("=========== ",qkey," QUERY ======================")
			print (qstring)
			print ("----------- ",qkey," RESULTS --------------------")
			for row in all_rows:
				print (row)
			print (" ")

			save_to_file = (re.search(r'q0\d', qkey) or re.search(r'q1[012]', qkey))
			if (save_to_file):
				with open(qkey+'.csv', 'w') as f:
					writer = csv.writer(f)
					writer.writerows(all_rows)
					f.close()
				print ("----------- ",qkey+".csv"," *SAVED* ----------------\n")
		
		except lite.Error as e:
			print ("An error occurred:", e.args[0])
	# DO NOT MODIFY - END
	
