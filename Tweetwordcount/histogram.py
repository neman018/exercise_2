import sys
import psycopg2

#Setup connection to the pre-created postgres database:

conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()
arg1, arg2 = sys.argv[1].split(",")

#This script returns all words with occurances between the first and second user-inputted argument:
cur.execute("SELECT word, count FROM tweetwordcount WHERE count between %s and %s ORDER BY count DESC",(arg1, arg2,))

records = cur.fetchall()

for rec in records:
	print rec[0]+':', rec[1]

conn.commit()
conn.close()
