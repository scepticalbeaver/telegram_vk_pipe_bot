# Author: Ivan Senin

import telepot
import time
import datetime as dt
import random
import threading
import logging

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
		logging.info("getMe request: %s", bot.getMe())

		self.db_client = db_ops.DBClient()
		self.users = self.db_client.fetch_users()
		logging.info("%d users were fetched from db", len(self.users))

	def start(self):
		self.answerer = telepot.helper.Answerer(bot)
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
						title="Get random quote!",
						input_message_content=InputTextMessageContent(
						message_text= quote
						)
						)]

				return (articles, 0)

		self.answerer.answer(msg, compute, quotes.get_quote())


	def on_chosen_inline_result(self, msg):
		result_id, from_id, query_string = telepot.glance(msg, flavor='chosen_inline_result')
		logging.info('Chosen Inline Result. result: %s\tfrom_id: %d\tquery: %s', result_id, from_id, query_string)


	def handle_private_chat(self, chat_id, msg):
		logging.info("handling private chat")
		is_new_user = False
		if chat_id in self.users:
			user = self.users[chat_id]
		else:
			user = User(chat_id, msg['chat']['first_name'], 0, True, False)
			self.users[chat_id] = user
			is_new_user = True
		seen = dt.datetime.fromtimestamp(user.last_seen)
		elapsed_time = dt.datetime.now() - seen
		user.update_seen_time()

		msg_text = msg['text'].split()
		if msg_text[-1].lower() == "updates":
			user.want_time = msg_text[1].lower() == "on"
		elif msg_text[-1].lower() == "notifications":
			user.muted = msg_text[1].lower() == "off"
		elif is_new_user or elapsed_time.seconds > 5 * 60:
			greeting = random.choice(SyncBot.greetings_first if is_new_user else SyncBot.greetings_next)
			self.bot.sendMessage(chat_id, user.name + "! " + greeting)
			if not is_new_user:
				self.bot.sendMessage(chat_id, "Haven't seen you for " + str(elapsed_time))
		switch_timer = "Switch {0} time updates".format("off" if user.want_time else "on")
		switch_sound = "Turn {0} notifications".format("on" if user.muted else "off")
		keyboard = ReplyKeyboardMarkup(keyboard=[
			[switch_timer],
			[switch_sound]
		])
		self.bot.sendMessage(chat_id, str(user), reply_markup=keyboard)
		self.db_client.update_user(user, is_new_one=is_new_user)

	def on_chat_message(self, msg):
		content_type, chat_type, chat_id = telepot.glance(msg)
		flavor = telepot.flavor(msg)
		logging.info("On chat message handler. Flavor: %s", flavor)
		logging.info("Message: %s", str(msg))

		if msg['chat']['type'] == "private" and content_type == "text":
			self.handle_private_chat(chat_id, msg)
		elif msg['chat']['type'] == "group" and content_type == "text":
			self.db_client.add_msg_from_tg(msg["message_id"], chat_id, msg["from"]["id"], msg["from"]["id"],
			                                content_type, msg["text"], msg["date"])
		else:
			logging.warning("Unsupported message. Content type: %s\tchat type: %s", content_type, chat_type)


if __name__ == "__main__":
	logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

	token_f = open("_token")

	_token = token_f.readline()
	if _token[-1] == '\n':
		_token = _token[:-1]

	bot = SyncBot(_token)

	bot.start()

	logging.info('Listening ...')
	while True:
		time.sleep(30)
		for id, user in users.iteritems():
			if user.want_time:
				logging.info("sending time to %d ...",  id)
				bot.sendMessage(id, "Current time: <b>" + str(dt.datetime.now().time().strftime('%H:%M:%S')) + "</b>",
								disable_notification=user.muted, parse_mode="html")




