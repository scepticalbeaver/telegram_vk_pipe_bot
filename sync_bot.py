#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ivan Senin

import datetime as dt
import logging
from pprint import pprint
import random
import telepot
import time
import threading
import Queue

from telepot.namedtuple import InlineQueryResultArticle, InputTextMessageContent, ReplyKeyboardMarkup

import db_ops
from chat_user import User
import quotes

class SyncBot(object):
	greetings_first = ["Hey!", "Hi!", "Good to see you!", "Nice to see you!", "It's nice to meet you!",
                   "Pleased to meet you!"]

	greetings_next = ["Hello again!", "Good to see you again!", "How's it going?", "How are you doing?", "What's up?",
                  "Pleased to meet you again!"]

	def __init__(self, token):
		assert isinstance(token, str)
		self.bot = telepot.Bot(token)
		logging.info("getMe request: %s", self.bot.getMe())

		self.answerer = telepot.helper.Answerer(self.bot)

		self.db_client = db_ops.DBClient("tg")
		self.users = self.db_client.fetch_users()
		logging.info("%d users were fetched from db", len(self.users))
		self.chats_to_monitor = self.db_client.get_monitored_chats()
		logging.info("%d chatd_ids to monitor were fetched from db", len(self.chats_to_monitor))
		self.msg_queue = Queue.Queue(120)
		self.new_users_to_register = Queue.Queue(15)
		self.users_mx = threading.Lock()


	def __event_loop(self):
		logging.info("Starting event loop")
		incoming_msg_handler = ChatMessagesHandler()
		users_update_handler = UserUpdatesHandler()
		unsync_messages_handler = UnsyncMessagesHandler()
		time_notification = TimeNotificationHandler()

		try:
			sleep_seconds = 1
			while True:
				incoming_msg_handler(msg_queue=self.msg_queue, db_client=self.db_client)
				users_update_handler(users=self.users, users_mx=self.users_mx, new_users=self.new_users_to_register,
						db_client=self.db_client)
				unsync_messages_handler(bot=self.bot, db_client=self.db_client)
				time_notification(bot=self.bot, users=self.users)

				time.sleep(sleep_seconds)
		except KeyboardInterrupt:
			logging.info("Event loop was interrupted by user")


	def start(self):
		error_counter = 0
		connected = False
		while not connected and error_counter < 5:
			try:
				self.bot.message_loop({'chat': self.on_chat_message, 'inline_query': self.on_inline_query,
									'chosen_inline_result': self.on_chosen_inline_result})
				connected = True
			except Exception as e:
				logging.error("Bot startup failed. Guess: %s", e.message)
				error_counter += 1
		if not connected:
			raise UserWarning("Cannot startup bot. Is it the only instance?")
		logging.info("Bot has been started up successfully")

		self.__event_loop()


	def on_inline_query(self, msg):
		logging.info("on_inline_query")
		mutex = threading.Lock()
		def compute(quote):
			logging.info("Compute's quote: %s", quote)
			with mutex:
				query_id, from_id, query_string = telepot.glance(msg, flavor='inline_query')
				logging.info("Inline query: query_id: %s, from_id: %d, query: %s", query_id, from_id, query_string)

				articles = [InlineQueryResultArticle(
						id='random_quote',
						title="Random quote!",
						input_message_content=InputTextMessageContent(
						message_text= quote
						)
						)]

				return (articles, 0)

		self.answerer.answer(msg, compute, quotes.get_quote())


	def on_chosen_inline_result(self, msg):
		result_id, from_id, query_string = telepot.glance(msg, flavor='chosen_inline_result')
		logging.info('Chosen Inline Result. result: %s\tfrom_id: %d\tquery: %s', result_id, from_id, query_string)

	def send_help_message(self, chat_id):
		text = "The synchrobot dublicates [text] messages to a chat in other platform working as a pipe. This way " \
		       "a <i>transchat</i> is introduced. It's capable to connect people who prefer to chat in different " \
		       "platforms\n" \
				"Also if you add @synchrobot to your contact list, you'll be able to pick a <b>random famous " \
		       "quote</b> in any chat via inline query technique: mention @synchrobot in the textfield and wait " \
				"for a button!\n" \
				"Enjoy!\n"

		logging.info("Usage message is sending to %d", chat_id)
		self.bot.sendMessage(chat_id, text, parse_mode="html")


	def handle_private_chat(self, chat_id, msg):
		logging.info("handling private chat")
		is_new_user = False
		if chat_id in self.users:
			with self.users_mx:
				user = self.users[chat_id]
		else:
			user = User(chat_id, msg['chat']['first_name'], 0, True, False)
			with self.users_mx:
				self.users[chat_id] = user
			is_new_user = True
			self.new_users_to_register.put(user)
			logging.info("New user was created: %s ", str(user))

		elapsed_time = dt.datetime.now() - dt.datetime.fromtimestamp(user.last_seen)
		user.update_seen_time()

		msg_text = msg['text'].split()
		if msg_text[-1].lower() == "updates":
			user.want_time = msg_text[1].lower() == "on"
		elif msg_text[-1].lower() == "notifications":
			user.muted = msg_text[1].lower() == "off"
		elif is_new_user or elapsed_time.seconds > 10 * 60:
			greeting = random.choice(SyncBot.greetings_first if is_new_user else SyncBot.greetings_next)
			self.bot.sendMessage(chat_id, user.name + "! " + greeting)
			if not is_new_user:
				self.bot.sendMessage(chat_id, "Haven't seen you for " + str(elapsed_time))
		elif msg_text[0] == "/help":
			self.send_help_message(chat_id)
		switch_timer = "Switch {0} time updates".format("off" if user.want_time else "on")
		switch_sound = "Turn {0} notifications".format("on" if user.muted else "off")
		keyboard = ReplyKeyboardMarkup(keyboard=[
			[switch_timer],
			[switch_sound],
			["/help"]
		])
		self.bot.sendMessage(chat_id, str(user), reply_markup=keyboard)


	def on_chat_message(self, msg):
		content_type, chat_type, chat_id = telepot.glance(msg)
		flavor = telepot.flavor(msg)
		logging.info("On chat message handler. Flavor: %s", flavor)
		#logging.info("Message: %s", str(msg))
		pprint(msg)

		if content_type == "text":
			if chat_id in self.chats_to_monitor:
				self.msg_queue.put(msg)
			if chat_type == "private":
				self.handle_private_chat(chat_id, msg)
			if chat_type == "group" and 'entities' in msg:
				for entity in msg['entities']:
					if entity['type'] == "bot_command":
						cmd = (msg["text"][entity['offset']:]).split()[0]
						if cmd == "\help":
							self.send_help_message()
						else:
							logging.info("Call for unsupported command: %s", cmd)
							self.bot.sendMessage("Unsupported command. Work in progress. Maybe. Maybe not. :grin:")


		else:
			logging.warning("Unsupported message. Content type: %s\tchat type: %s", content_type, chat_type)


class ChatMessagesHandler(db_ops.Handler):
	def __init__(self):
		super(ChatMessagesHandler, self).__init__()
		self.period = dt.timedelta(seconds=13)

	def handler_hook(self, **kwargs):
		counter = 20
		while not kwargs["msg_queue"].empty() and counter > 0:
			counter -= 1
			msg = kwargs["msg_queue"].get()
			content_type, chat_type, chat_id = telepot.glance(msg)
			logging.info("ChatMessagesHandler: flushing to db msg: %s", str(msg))
			kwargs["db_client"].add_msg(msg["message_id"], chat_id, msg["from"]["id"], msg["from"]["first_name"],
					msg["from"]["username"], content_type, msg["text"], msg["date"])


class UserUpdatesHandler(db_ops.Handler):
	def __init__(self):
		super(UserUpdatesHandler, self).__init__()
		self.period = dt.timedelta(seconds=25)

	def handler_hook(self, **kwargs):
		users_mx = kwargs["users_mx"]
		users = kwargs["users"].values()
		new_users = kwargs["new_users"]
		db_client = kwargs["db_client"]

		counter = 15
		while not new_users.empty() and counter > 0:
			counter -= 1
			user = new_users.get()
			logging.info("UserUpdatesHandler: flushing to db new user: (%d, %s)", user.id, user.name)
			db_client.update_user(user, is_new_one=True)
			user.dirty = False
		if counter > 0:
			with users_mx:
				for user in users:
					if user.dirty:
						logging.info("UserUpdatesHandler: flushing to db dirty user: (%d, %s)", user.id, user.name)
						db_client.update_user(user)
						user.dirty = False


class UnsyncMessagesHandler(db_ops.Handler):
	def __init__(self):
		super(UnsyncMessagesHandler, self).__init__()
		self.period = dt.timedelta(seconds=11)

	def handler_hook(self, **kwargs):
		db_client = kwargs["db_client"]
		bot = kwargs["bot"]
		for row_dict in db_client.fetch_unsync_messages():
			logging.info("Sending unsync message: %s ", str(row_dict))
			msg_time = dt.datetime.fromtimestamp(row_dict["date"]).strftime('%H:%M:%S')
			msg_text = "{0} ({1}), {2}: {3}".format(row_dict["sender_name"].encode('utf-8'),
					row_dict["username"].encode('utf-8'), msg_time, row_dict["content"].encode('utf-8'))
			bot.sendMessage(row_dict["tg_chat_id"], msg_text)


class TimeNotificationHandler(db_ops.Handler):
	def __init__(self):
		super(TimeNotificationHandler, self).__init__()
		self.period = dt.timedelta(seconds=20)

	def handler_hook(self, **kwargs):
		users = kwargs["users"]
		bot = kwargs["bot"]
		for id, user in users.iteritems():
				if user.want_time:
					logging.info("TimeNotificationHandler: sending time to (%d, %s) ...",  id, user.name)
					bot.sendMessage(id, "Current time: <b>" + str(dt.datetime.now().time().strftime('%H:%M:%S')) +
							"</b>", disable_notification=user.muted, parse_mode="html")



if __name__ == "__main__":
	logging.basicConfig(format='tg side: %(asctime)s %(message)s', level=logging.INFO)

	token_f = open("_token")
	_token = token_f.readline()
	if _token[-1] == '\n':
		_token = _token[:-1]

	bot = SyncBot(_token)

	bot.start()





