import sys
import psycopg2

#Setup connection to the pre-created postgres database:

conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()

#Check the script's arguments and then run the corresponding query.

if len(sys.argv) == 1:
#If the script is run with no arguments, then retrun all the words in the stream and the total count of occurances, sorted alphabetically.

	cur.execute("SELECT word, count FROM tweetwordcount ORDER BY word ASC")
	records = cur.fetchall()
	
	for rec in records:
		print "(",rec[0]+',', rec[1],")"
else:
#If there is a word argument, I will print the total number of occurances for that specific word.

	cur.execute("SELECT word, count FROM tweetwordcount WHERE word = %s ORDER BY word ASC",(sys.argv[1],))
	records = cur.fetchall()

	for rec in records:
		print "Total number of occurences of ",rec[0] + ':', rec[1], "\n"
