import sqlite3
import datetime

from user import User

DB_NAME = 'ip_sink_data.db'


def create_fresh_db():
	conn = sqlite3.connect(DB_NAME)

	c = conn.cursor()

	try:
		c.execute('''DROP TABLE  users''')
		c.execute('''DROP TABLE  messages''')
		conn.commit()
	except Exception as e:
		print e.message

	c.execute('''CREATE TABLE users
	             (user_id integer PRIMARY KEY NOT NULL,
	              name TEXT,
	              last_contact_date DATE,
	              want_time boolean,
	              mute_dialog boolean)''')

	c.execute('''CREATE TABLE messages
	             (message_id INT NOT NULL,
	              chat_id INT NOT NULL,
	              source TEXT,
	              sender_id INT,
	              sender_name TEXT,
	              msg_type TEXT,
	              content TEXT,
	              date DATE,
	              SYNCED BOOLEAN,
	              PRIMARY KEY (message_id, chat_id))''')

	#c.execute("INSERT INTO users VALUES (?, 'IVAN', ?, 'TRUE', 'FALSE')", (50704733, 1472493782))

	conn.commit()
	conn.close()
	print "Done"


def update_user(user, is_new_one=False):
	if not isinstance(user, User):
		raise Exception("don't want to update strange thing")

	conn = sqlite3.connect(DB_NAME)
	c = conn.cursor()
	if is_new_one:
		c.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?)", (user.id, user.name, user.last_seen, user.want_time,
																user.muted))
	else:
		c.execute("UPDATE users SET name = ?, last_contact_date = ?, want_time = ?, mute_dialog = ? WHERE user_id = ?",
					(user.name, user.last_seen, user.want_time, user.muted, user.id))

	conn.commit()
	conn.close()


def fetch_users():
	conn = sqlite3.connect(DB_NAME)
	c = conn.cursor()
	users = {}
	for row in c.execute("SELECT * FROM users"):
		id = int(row[0])
		name = row[1]
		last_seen = int(row[2])
		want_time = bool(row[3])
		muted = bool(row[4])
		users[id] = User(id, name, last_seen, want_time, muted)

	return users