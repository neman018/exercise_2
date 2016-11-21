from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt

import psycopg2

class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()

        #Connect to the database
	self.conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")

        #Create a cursor:
	self.cur = self.conn.cursor()

	self.cur.execute('''DROP TABLE IF EXISTS tweetwordcount''')
	self.conn.commit()

	self.cur.execute("""CREATE TABLE tweetwordcount
	(word TEXT PRIMARY KEY     NOT NULL,
	count INT     NOT NULL);""")

	self.conn.commit()
	#self.conn.close()



    def process(self, tup):
        word = tup.values[0]
        # Use psycopg to interact with Postgres
        # Database name: tcount 
        # Table name: tweetwordcount 
        # Both the database and the table were created in advance.
        

        # Increment the local count
        self.counts[word] += 1
        self.emit([word, self.counts[word]])

	if self.counts[word] == 1:
		self.cur.execute("INSERT INTO tweetwordcount (word,count) VALUES (%s, %s)", (word,1))
	else:
		self.cur.execute("UPDATE tweetwordcount SET count=count+1 WHERE word=%s", (word,))
	self.conn.commit()

	if self.counts[word] > 0:
		self.emit([word, self.counts[word]])

        # Log the count - just to see the topology running
        self.log('%s: %d' % (word, self.counts[word]))
