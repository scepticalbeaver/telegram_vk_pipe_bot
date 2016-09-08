#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ivan Senin

import calendar
import datetime as dt
import logging
import os
import sqlite3
import time

from synchrobot.chat_user import User


class DBClient(object):
	DB_NAME = 'pipe_data.db'
	SUPPORTED_PLATFORMS = ["vk", "tg"]

	def __init__(self, bot_platform):
		assert bot_platform in self.SUPPORTED_PLATFORMS, "Unsupported platform"
		self.__platform = bot_platform
		self.logger = logging.getLogger(__name__ + "(" +self.__platform + ")")
		have_saved_data = os.path.isfile(self.DB_NAME)
		self.logger.info("Connecting to %s ...", DBClient.DB_NAME)
		self.conn = sqlite3.connect(DBClient.DB_NAME)
		if not have_saved_data:
			self.logger.info("%s doesn't exist. Creating new one ...", self.DB_NAME)
			self.create_fresh_db()

	def create_fresh_db(self):
		c = self.conn.cursor()

		try:
			c.execute('''DROP TABLE users''')
			c.execute('''DROP TABLE messages''')
			c.execute('''DROP TABLE msg_pipe''')
			c.execute('''DROP TABLE online_stats''')
			self.conn.commit()
		except Exception as e:
			self.logger.warning("Failed to drop tables. Reason: %s", e.message)

		c.execute('''CREATE TABLE users
					(user_id INT NOT NULL,
					name TEXT,
					last_contact_date DATE,
					want_time BOOLEAN,
					mute_dialog BOOLEAN,
					platform TEXT CHECK(platform = 'vk' or platform = 'tg'),
					username TEXT,
					PRIMARY KEY (user_id, platform))''')

		c.execute('''CREATE TABLE messages
					(internal_id INTEGER PRIMARY KEY AUTOINCREMENT,
					message_id INT NOT NULL,
					tg_chat_id INT,
					vk_chat_id INT,
					sender_id INT NOT NULL,
					sender_name TEXT,
					username TEXT,
					msg_type TEXT,
					content TEXT,
					date DATE)''')

		c.execute('''CREATE TABLE msg_pipe
						(id INTEGER PRIMARY KEY AUTOINCREMENT,
						tg_chat_id INT,
						vk_chat_id INT,
						is_active BOOLEAN DEFAULT 0,
						code TEXT,
						UNIQUE (tg_chat_id, vk_chat_id))''')

		c.execute('''CREATE TABLE online_stats
					(user_id INTEGER REFERENCES users(user_id),
					is_online BOOLEAN NOT NULL,
					using_mobile BOOLEAN NOT NULL,
					timing DATE NOT NULL)''')

		self.conn.commit()
		self.logger.info("Brand new tables were created")

	def update_user(self, users, is_new_ones=False):
		if not users:
			return
		if isinstance(users, User):
			users = [users]
		if not isinstance(users[0], User):
			raise ValueError("don't want to update strange thing")

		c = self.conn.cursor()
		for user in users:
			if is_new_ones:
				c.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?)",
						(user.id, user.name, user.last_seen, user.want_time, user.muted, self.__platform,
						 user.username))
			else:
				c.execute("UPDATE users SET name = ?, last_contact_date = ?, want_time = ?, mute_dialog = ?" +
				          " WHERE user_id = ? AND platform = ?",
				          (user.name, user.last_seen, user.want_time, user.muted, user.id, self.__platform))
		self.conn.commit()
		self.logger.info("%d users were flushed to db", len(users))

	def fetch_users(self):
		c = self.conn.cursor()
		users = {}
		for row in c.execute("SELECT * FROM users WHERE platform = ?", (self.__platform,)):
			id = int(row[0])
			name = row[1]
			last_seen = int(row[2])
			want_time = bool(row[3])
			muted = bool(row[4])
			username = row[6]
			users[id] = User(id, name, last_seen, want_time, muted, username)
			users[id].dirty = False
		return users

	def add_msg(self, msg_id, chat_id, sender_id, sender_name, username, msg_type, content, date):
		chat_id_placeholder = "?, NULL" if self.__platform == 'tg' else "NULL, ?"
		c = self.conn.cursor()
		c.execute("INSERT INTO messages VALUES (NULL, ?, " + chat_id_placeholder + ", ?, ?, ?, ?, ?, ?)",
				(msg_id, chat_id, sender_id, sender_name, username, msg_type, content, date))

		self.conn.commit()

	def fetch_unsync_messages(self, do_update=True):
		# a generator

		curr_chat_id = self.__platform + "_chat_id"
		other_chat_id = ("vk" if self.__platform == "tg" else "tg") + "_chat_id"

		c = self.conn.cursor()
		c.execute("SELECT date, sender_name, username, content, msg_pipe." + curr_chat_id + ", internal_id" +
					" FROM messages " +
					"JOIN msg_pipe ON messages." + other_chat_id + " = msg_pipe." + other_chat_id +
					" WHERE messages." + curr_chat_id + " is NULL")

		rows = c.fetchall()
		for row in rows:
			row_dict = {"date": row[0], "sender_name": row[1], "username": row[2],
						"content": row[3], curr_chat_id: row[4]}
			yield row_dict
			if do_update and "sent" in row_dict:
				internal_msg_id = row[5]
				c.execute("UPDATE messages SET " + curr_chat_id + " = ? WHERE internal_id = ? ",
						(row_dict[curr_chat_id], internal_msg_id))
				self.conn.commit()

	def get_monitored_chats(self):
		c = self.conn.cursor()
		c.execute("SELECT " + self.__platform + "_chat_id FROM msg_pipe WHERE is_active == 1")
		return [row[0] for row in c.fetchall()]

	def set_pending_chat(self, tg_chat_id, vk_chat_id, code):
		assert self.__platform == "tg", "Pipe could be established only from telegram"
		assert isinstance(code, str), "activation code must be a string"
		c = self.conn.cursor()
		c.execute("INSERT INTO msg_pipe VALUES(NULL, ?, ?, 0, ?)", (tg_chat_id, vk_chat_id, code))
		self.conn.commit()

	def get_pending_chat_ids(self):
		c = self.conn.cursor()
		c.execute("SELECT * FROM msg_pipe WHERE is_active = 0")
		rows = c.fetchall()
		pending_chats_d = {}
		for row in rows:
			pending_chats_d[row[2 if self.__platform == "vk" else 1]] = row[4]
		return pending_chats_d

	def remove_pipe(self, tg_chat_id):
		c = self.conn.cursor()
		c.execute("DELETE FROM msg_pipe WHERE tg_chat_id = ?", (tg_chat_id,))
		self.conn.commit()

	def check_pending_chats(self, code):
		assert isinstance(code, str) or isinstance(code, unicode), "activation code must be a string"
		code = str(code)
		c = self.conn.cursor()
		c.execute("SELECT * FROM msg_pipe WHERE code = ? AND is_active = 0", (code,))
		rows = c.fetchall()
		for row in rows:
			row_dict = {"id": row[0], "tg_chat_id": row[1], "vk_chat_id": row[2]}
			yield  row_dict
			if "confirmed" in row_dict:
				c.execute("UPDATE msg_pipe SET is_active = 1 WHERE id = ?", (int(row_dict['id']),))
				c.execute("DELETE FROM msg_pipe WHERE tg_chat_id = ? AND id != ?",
						(int(row_dict['tg_chat_id']), int(row_dict['id'])))
				self.conn.commit()
				break

	def append_users_observations(self, users_to_state_d):
		current_ts = calendar.timegm(time.gmtime())
		c = self.conn.cursor()
		for user, (is_online, using_mobile) in users_to_state_d.iteritems():
			c.execute("INSERT INTO online_stats VALUES (?, ?, ?, ?)", (user.id, is_online, using_mobile, current_ts))
		self.conn.commit()

	def close(self):
		self.conn.close()
		self.logger.info("Connection to %s closed", self.DB_NAME)


class Handler(object):
	def __init__(self, db_client=None, api=None):
		self.period = None  # you must set this in inherited class
		self.time_to_go = dt.datetime.now()
		self.db_client = db_client
		self.api = api

	def handler_hook(self, **kwargs):
		pass

	def __call__(self, *args, **kwargs):
		# Do not override this method. Use a hook
		if dt.datetime.now() > self.time_to_go:
			result = self.handler_hook(**kwargs)
			self.time_to_go = dt.datetime.now() + self.period
			return result


class UserUpdatesHandler(Handler):
	def __init__(self, db_client, users_d):
		super(UserUpdatesHandler, self).__init__(db_client)
		self.period = dt.timedelta(seconds=20)
		self.logger = logging.getLogger(__name__)
		self.users = users_d.values()

	def handler_hook(self, **kwargs):
		users_mx = kwargs["users_mx"]
		new_users = kwargs["new_users"]

		counter = 30
		new_users_l = []
		while not new_users.empty() and counter > 0:
			counter -= 1
			user = new_users.get()
			self.logger.info("UserUpdatesHandler: flushing to db new user: (%d, %s)", user.id, user.name)
			user.dirty = False
			new_users_l.append(user)
		self.db_client.update_user(new_users_l, is_new_ones=True)

		with users_mx:
			users_to_update = filter(lambda user: user.dirty, self.users)[:max(0, counter)]
			for user in users_to_update:
				self.logger.info("UserUpdatesHandler: flushing to db dirty user: (%d, %s)", user.id, user.name)
				user.dirty = False
			self.db_client.update_user(users_to_update)
