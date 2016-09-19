#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ivan Senin

import Queue
import collections
import datetime as dt
import logging
import random
import string
import threading
import time

import telepot
from telepot.namedtuple import InlineQueryResultArticle, InputTextMessageContent, ReplyKeyboardMarkup

from synchrobot import db_ops, quotes
from synchrobot.chat_user import User


class SyncBot(object):
	greetings_first = ["Hey!", "Hi!", "Good to see you!", "Nice to see you!", "It's nice to meet you!",
                   "Pleased to meet you!"]

	greetings_next = ["Hello again!", "Good to see you again!", "How's it going?", "How are you doing?", "What's up?",
                  "Pleased to meet you again!"]
	LONGPOLL_RETRY_RELAX_SECONDS = .7
	VK_GROUP_IDS = 2000000000

	def __init__(self, token):
		self.logger = logging.getLogger(__name__)
		assert isinstance(token, str)
		self.bot = LimitsAwareBot(token)
		self.logger.info("getMe request: %s", self.bot.getMe())

		self.answerer = telepot.helper.Answerer(self.bot)

		self.db_client = db_ops.DBClient("tg")
		self.users = self.db_client.fetch_users()
		self.logger.info("%d users were fetched from db", len(self.users))
		self.chats_to_monitor = self.db_client.get_monitored_chats()
		self.logger.info("%d chatd_ids to monitor were fetched from db", len(self.chats_to_monitor))
		self.msg_queue = Queue.Queue()
		self.new_users_to_register = Queue.Queue(15)
		self.users_mx = threading.Lock()
		self.chats_to_activate = Queue.Queue()


	def __event_loop(self, stop_signal_q):
		self.logger.info("Starting event loop")
		incoming_msg_handler = ChatMessagesHandler(self.db_client)
		users_update_handler = db_ops.UserUpdatesHandler(self.db_client, self.users)
		unsync_messages_handler = UnsyncMessagesHandler(self.db_client, self.bot)
		time_notification = TimeNotificationHandler(self.bot)
		pipe_control = PipeControlHandler(self.db_client, self.chats_to_activate, self.bot)

		try:
			sleep_seconds = 0.3
			while stop_signal_q.empty():
				res = pipe_control()
				if not res is None:
					self.chats_to_monitor = res
				incoming_msg_handler(msg_queue=self.msg_queue)
				time_notification(users=self.users)
				unsync_messages_handler()
				users_update_handler(users_mx=self.users_mx, new_users=self.new_users_to_register)

				time.sleep(sleep_seconds)
		except KeyboardInterrupt:
			self.logger.info("Event loop was interrupted by user")

	def start(self, stop_signal_q = Queue.Queue()):
		error_counter = 0
		connected = False
		dispatch = {'chat': self.on_chat_message, 'edited_chat': self.on_edited_message, 'inline_query': self.on_inline_query,
				'chosen_inline_result': self.on_chosen_inline_result}
		while not connected and error_counter < 5:
			try:
				self.bot.message_loop(callback=dispatch, relax=self.LONGPOLL_RETRY_RELAX_SECONDS)
				connected = True
			except BaseException as e:
				self.logger.exception("Bot startup failed. Guess: %s", e.message)
				error_counter += 1
		if not connected:
			raise UserWarning("Cannot startup bot. Is it the only instance?")
		self.logger.info("Bot has been started up successfully")

		self.__event_loop(stop_signal_q)
		self.db_client.close()
		if not stop_signal_q.empty():
			self.logger.info("Execution was stopped via stop-event")


	def on_inline_query(self, msg):
		self.logger.info("on_inline_query")
		mutex = threading.Lock()

		def compute(quote):
			self.logger.info("Compute's quote: %s", quote)
			with mutex:
				query_id, from_id, query_string = telepot.glance(msg, flavor='inline_query')
				self.logger.info("Inline query: query_id: %s, from_id: %d, query: %s", query_id, from_id, query_string)

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
		self.logger.info('Chosen Inline Result. result: %s\tfrom_id: %d\tquery: %s', result_id, from_id, query_string)

	def send_help_message(self, chat_id):
		text = "The synchrobot dublicates [text] messages to a chat in other platform working as a pipe. This way " \
		       "a <i>transchat</i> is introduced. It's capable to connect people who prefer to chat in different " \
		       "platforms\n" \
				"Also if you add @synchrobot to your contact list, you'll be able to pick a <b>random famous " \
		       "quote</b> in any chat via inline query technique: mention @synchrobot in the textfield and wait " \
				"for a button!\n" \
				"Moreover, you can setup a pipe to a single vk-user via command /install_pipe_private vk_id\n" \
				"Send /uninstall to remove current pipe\n" \
				"Enjoy!\n"

		self.logger.info("Usage message is sending to chat_id %d", chat_id)
		self.bot.sendMessage(chat_id, text, parse_mode="html")

	def handle_private_chat(self, chat_id, msg):
		self.logger.info("handling private chat")

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
			self.logger.info("New user was created: %s ", str(user))

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

		switch_timer = "Switch {0} time updates".format("off" if user.want_time else "on")
		switch_sound = "Turn {0} notifications".format("on" if user.muted else "off")
		keyboard = ReplyKeyboardMarkup(keyboard=[
			[switch_timer],
			[switch_sound],
			["/help"]
		])
		if not chat_id in self.chats_to_monitor:
			self.bot.sendMessage(chat_id, str(user), reply_markup=keyboard)

	def on_chat_message(self, msg):
		content_type, chat_type, chat_id = telepot.glance(msg)
		flavor = telepot.flavor(msg)
		self.logger.info("On chat message handler. Flavor: %s, chat_id: %d", flavor, chat_id)

		if content_type == "text":
			if chat_id in self.chats_to_monitor:
				self.msg_queue.put(msg)

			if 'entities' in msg:
				for entity in msg['entities']:
					if entity['type'] == "bot_command":
						cmd = (msg["text"][entity['offset']:]).split()[0]
						if cmd == "/help":
							self.send_help_message(chat_id)
						elif cmd == "/start" and chat_type == "private":
							self.handle_private_chat(chat_id, msg)
						elif "/install_pipe" in cmd:
							try:
								vk_chat_id = int((msg["text"][entity['offset']:]).split()[1])
							except:
								self.bot.sendMessage(chat_id, "Unsuccessful. Please see /help message")
								return
							if cmd != "/install_pipe_private":
								vk_chat_id += self.VK_GROUP_IDS
							self.chats_to_activate.put((chat_id, vk_chat_id, True))
						elif cmd == "/uninstall":
							self.chats_to_activate.put((chat_id, -1, False))
						else:
							self.logger.info("Call for unsupported command: %s", cmd)
							reply_unsupported = "Unsupported command. Work in progress. Maybe. Maybe not."
							self.bot.sendMessage(chat_id, reply_unsupported)
			elif chat_type == "private":
				self.handle_private_chat(chat_id, msg)
		else:
			self.logger.warning("Unsupported message. Content type: %s\tchat type: %s", content_type, chat_type)

	def on_edited_message(self, msg):
		content_type, chat_type, chat_id = telepot.glance(msg)
		self.logger.warning("Edited message ignored. Chat id: %d", chat_id)


class ChatMessagesHandler(db_ops.Handler):
	def __init__(self, db_client):
		super(ChatMessagesHandler, self).__init__(db_client)
		self.period = dt.timedelta(seconds=2)
		self.logger = logging.getLogger(__name__)

	def handler_hook(self, **kwargs):
		counter = 20
		while not kwargs["msg_queue"].empty() and counter > 0:
			counter -= 1
			msg = kwargs["msg_queue"].get()
			content_type, chat_type, chat_id = telepot.glance(msg)
			self.logger.info("ChatMessagesHandler: flushing to db msg: %s", str(msg))
			self.db_client.add_msg(msg["message_id"], chat_id, msg["from"]["id"], msg["from"]["first_name"],
					msg["from"]["username"], content_type, msg["text"], msg["date"])


class UnsyncMessagesHandler(db_ops.Handler):
	def __init__(self, db_client, bot):
		super(UnsyncMessagesHandler, self).__init__(db_client, bot)
		self.period = dt.timedelta(seconds=4)
		self.logger = logging.getLogger(__name__)

	def handler_hook(self, **kwargs):
		counter = 3
		for row_dict in self.db_client.fetch_unsync_messages():
			counter -= 1
			self.logger.info("Sending unsync message: %s ", str(row_dict))
			msg_time = dt.datetime.fromtimestamp(row_dict["date"]).strftime('%H:%M:%S')
			msg_text = "{0} ({1}), {2}: {3}".format(row_dict["sender_name"].encode('utf-8'),
					row_dict["username"].encode('utf-8'), msg_time, row_dict["content"].encode('utf-8'))
			chat_id = row_dict["tg_chat_id"]
			if not self.api.is_hitting_limits(chat_id) and counter > 0:
				if not self.api.sendMessage(chat_id, msg_text):
					return
				row_dict['sent'] = True
				time.sleep(1)
			else:
				break


class TimeNotificationHandler(db_ops.Handler):
	def __init__(self, bot):
		super(TimeNotificationHandler, self).__init__(api=bot)
		self.period = dt.timedelta(seconds=20)
		self.logger = logging.getLogger(__name__)

	def handler_hook(self, **kwargs):
		users = kwargs["users"]
		for id, user in users.iteritems():
				if user.want_time:
					self.logger.info("TimeNotificationHandler: sending time to (%d, %s) ...",  id, user.name)
					self.api.sendMessage(id, "Current time: <b>" + str(dt.datetime.now().time().strftime('%H:%M:%S')) +
							"</b>", disable_notification=user.muted, parse_mode="html")


class PipeControlHandler(db_ops.Handler):
	def __init__(self, db_client, control_msg_q, bot):
		super(PipeControlHandler, self).__init__(db_client, bot)
		self.period = dt.timedelta(seconds=5)
		self.logger = logging.getLogger(__name__)
		self.control_msg_q = control_msg_q

	def handler_hook(self, **kwargs):
		while not self.control_msg_q.empty():
			tg_chat_id, vk_chat_id, want_install = self.control_msg_q.get()
			self.logger.info("PipeControlHandler: request for a pipe update: tg:%d <> vk:%d, is_new_one (%d)",
					tg_chat_id, vk_chat_id, want_install)
			if want_install:
				activation_code = '/' +  ''.join(random.choice
						(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(8))
				try:
					self.db_client.set_pending_chat(tg_chat_id, vk_chat_id, activation_code)
					reply_text = "Please confirm the pipe on the other end by sending the following confirmation code:"\
							" " + activation_code
				except UserWarning as w:
					reply_text = "The pipe is already exist. Maybe you want `/uninstall` first?"
				except Exception as e:
					self.logger.exception("PipeControlHandler: cannot setup pending chat. Reason: %s", e.message)
					reply_text = "Unsuccessful operation"
				self.api.sendMessage(tg_chat_id, reply_text)
			else:
				self.db_client.remove_pipe(tg_chat_id)
		return self.db_client.get_monitored_chats()


class LimitsAwareBot(telepot.Bot):
	ALLOWED_PER_MIN = 20

	def __init__(self, *args, **kwargs):
		super(LimitsAwareBot, self).__init__(*args, **kwargs)
		self.logger = logging.getLogger(__name__)
		self.outpost_timings = {}

	def is_hitting_limits(self, chat_id):
		if chat_id not in self.outpost_timings:
			self.outpost_timings[chat_id] = collections.deque([dt.datetime.fromtimestamp(0)] * 20)
		return (dt.datetime.now() - self.outpost_timings[chat_id][-1]).seconds < 1 \
				or (dt.datetime.now() - self.outpost_timings[chat_id][-self.ALLOWED_PER_MIN]).seconds < 60

	def sendMessage(self, chat_id, *args, **kwargs):
		"""
		:return: True if a deliver was successful, False otherwise
		"""
		try:
			super(LimitsAwareBot, self).sendMessage(chat_id, *args, **kwargs)
		except telepot.exception.TelepotException as e:
			self.logger.error("Cannot deliver a message. Reason: %s", e.message)
			return False
		except BaseException as be:
			self.logger.exception("Unexpected excepton: %s", be.message)
			return False
		if chat_id not in self.outpost_timings:
			self.outpost_timings[chat_id] = collections.deque([dt.datetime.fromtimestamp(0)] * 20)
		self.outpost_timings[chat_id].append(dt.datetime.now())
		self.outpost_timings[chat_id].popleft()
		return True


if __name__ == "__main__":
	logging.basicConfig(format='%(asctime)s:%(levelname)s:%(name)s:%(message)s', level=logging.INFO)

	token_f = open("_token")
	_token = token_f.readline()
	_token = _token.replace('\n', '')

	bot = SyncBot(_token)

	bot.start()
