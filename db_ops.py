import sqlite3
import datetime
import logging
import os

from chat_user import User

class DBClient(object):
	DB_NAME = 'ip_sink_data.db'

	def __init__(self):
		if os._exists(DBClient.DB_NAME):
			logging.info("Connecting to %s ...", DBClient.DB_NAME)
			self.conn = sqlite3.connect(DBClient.DB_NAME)
		else:
			logging.info("%s doesn't exist. Creating new one ...")
			self.create_fresh_db()

	def create_fresh_db(self):
		c = self.conn.cursor()

		try:
			c.execute('''DROP TABLE  users''')
			c.execute('''DROP TABLE  messages''')
			c.execute('''DROP TABLE  msg_pipe''')
			self.conn.commit()
		except Exception as e:
			logging.error("Failed to drop tables. Reason: %s", e.message)

		c.execute('''CREATE TABLE users
		             (user_id integer PRIMARY KEY NOT NULL,
		              name TEXT,
		              last_contact_date DATE,
		              want_time boolean,
		              mute_dialog boolean)''')

		c.execute('''CREATE TABLE messages
		             (message_id INT NOT NULL,
		              tg_chat_id INT,
		              vk_chat_id INT,
		              sender_id INT NOT NULL,
		              sender_name TEXT,
		              msg_type TEXT,
		              content TEXT,
		              date DATE,
		              PRIMARY KEY (message_id, sender_id))''')

		c.execute('''CREATE TABLE msg_pipe
						(tg_chat_id INT NOT NULL,
						vk_chat_id INT NOT NULL,
						PRIMARY KEY (tg_chat_id, vk_chat_id))''')

		self.conn.commit()
		logging.info("Brand new tables were created")


	def update_user(self, user, is_new_one=False):
		if not isinstance(user, User):
			raise ValueError("don't want to update strange thing")

		c = self.conn.cursor()
		if is_new_one:
			c.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?)", (user.id, user.name, user.last_seen, user.want_time,
																	user.muted))
		else:
			c.execute("UPDATE users SET name = ?, last_contact_date = ?, want_time = ?, mute_dialog = ? WHERE user_id = ?",
						(user.name, user.last_seen, user.want_time, user.muted, user.id))

		self.conn.commit()


	def fetch_users(self):
		c = self.conn.cursor()
		users = {}
		for row in c.execute("SELECT * FROM users"):
			id = int(row[0])
			name = row[1]
			last_seen = int(row[2])
			want_time = bool(row[3])
			muted = bool(row[4])
			users[id] = User(id, name, last_seen, want_time, muted)

		return users

	def add_msg_from_tg(self, msg_id, tg_chat_id, sender_id, sender_name, msg_type, content, date):
		c = self.conn.cursor()
		c.execute("INSERT INTO messages VALUES (?, ?, NULL, ?, ?, ?, ?, ?, 0)", (msg_id, tg_chat_id, sender_id,
			sender_name, msg_type, content, date))

		self.conn.commit()

	def fetch_unsync_messages_from_vk(self, do_update=True):
		c = self.conn.cursor()
		c.execute("SELECT date, sender_name, content, msg_pipe.tg_chat_id, message_id, sender_id  FROM messages "
		          "JOIN msg_pipe ON messages.vk_chat_id = msg_pipe.vk_chat_id WHERE messages.tg_chat_id is NULL")

		rows = c.fetchall()
		for row in rows:
			d = {"date": row[0], "sender" : row[1], "content" : row[2], "tg_chat_id" : row[3]}
			yield d
			if do_update:
				msg_id = row[4]
				sender_id = row[5]
				c.execute("UPDATE messages SET tg_chat_id = ? WHERE message_id = ? AND sender_id = ?",
						(d["tg_chat_id"], msg_id, sender_id))
				self.conn.commit()

	def close(self):
		self.conn.close()