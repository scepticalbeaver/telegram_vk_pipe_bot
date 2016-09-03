#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import datetime as dt
import logging
import os

from chat_user import User


class DBClient(object):
	DB_NAME = 'ip_sink_data.db'
	SUPPORTED_PLATFORMS = ["vk", "tg"]

	def __init__(self, bot_platform):
		assert bot_platform in self.SUPPORTED_PLATFORMS, "Unsupported platform"
		self.__platform = bot_platform
		have_saved_data = os.path.isfile(self.DB_NAME)
		logging.info("Connecting to %s ...", DBClient.DB_NAME)
		self.conn = sqlite3.connect(DBClient.DB_NAME)
		if not have_saved_data:
			logging.info("%s doesn't exist. Creating new one ...", self.DB_NAME)
			self.create_fresh_db()

	def create_fresh_db(self):
		c = self.conn.cursor()

		try:
			c.execute('''DROP TABLE users''')
			c.execute('''DROP TABLE messages''')
			c.execute('''DROP TABLE msg_pipe''')
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
					username TEXT,
					msg_type TEXT,
					content TEXT,
					date DATE,
					PRIMARY KEY (message_id, sender_id))''')

		c.execute('''CREATE TABLE msg_pipe
						(id INTEGER PRIMARY KEY AUTOINCREMENT,
						tg_chat_id INT,
						vk_chat_id INT,
						is_active BOOLEAN DEFAULT 0,
						UNIQUE (tg_chat_id, vk_chat_id))''')
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
			users[id].dirty = False
		return users

	def add_msg(self, msg_id, chat_id, sender_id, sender_name, username, msg_type, content, date):
		chat_id_placeholder = "?, NULL" if self.__platform == 'tg' else "NULL, ?"
		c = self.conn.cursor()
		c.execute("INSERT INTO messages VALUES (?, " + chat_id_placeholder + ", ?, ?, ?, ?, ?, ?)",
				(msg_id, chat_id, sender_id, sender_name, username, msg_type, content, date))

		self.conn.commit()

	def fetch_unsync_messages(self, do_update=True):
		# a generator

		curr_chat_id = self.__platform + "_chat_id"
		other_chat_id = ("vk" if self.__platform == "tg" else "tg") + "_chat_id"

		c = self.conn.cursor()
		c.execute("SELECT date, sender_name, username, content, msg_pipe." + curr_chat_id + ", message_id " +
					" FROM messages " +
					"JOIN msg_pipe ON messages." + other_chat_id + " = msg_pipe." + other_chat_id +
					" WHERE messages." + curr_chat_id + " is NULL")

		rows = c.fetchall()
		for row in rows:
			row_dict = {"date": row[0], "sender_name": row[1], "username": row[2],
						"content": row[3], curr_chat_id: row[4]}
			yield row_dict
			if do_update:
				msg_id = row[5]

				c.execute("UPDATE messages SET " + curr_chat_id + " = ? WHERE message_id = ? AND " +
							 curr_chat_id + " is NULL",
						(row_dict[curr_chat_id], msg_id))
				self.conn.commit()


	def get_monitored_chats(self):
		c = self.conn.cursor()

		c.execute("SELECT " + self.__platform + "_chat_id FROM msg_pipe WHERE is_active == 1")
		return [row[0] for row in c.fetchall()]

	def close(self):
		self.conn.close()


class Handler(object):
	def __init__(self):
		self.time_to_go = dt.datetime.now()
		self.period = None  # you must set this in inherited class

	def handler_hook(self, **kwargs):
		pass

	def __call__(self, *args, **kwargs):
		# Do not override this method. Use a hook
		if dt.datetime.now() > self.time_to_go:
			self.handler_hook(**kwargs)
			self.time_to_go = dt.datetime.now() + self.period
