# Author: Ivan Senin

import telepot

from pprint import pprint
from telepot.namedtuple import InlineQueryResultArticle, InputTextMessageContent, ReplyKeyboardMarkup, KeyboardButton

from telepot.delegate import per_inline_from_id, create_open
import time
import datetime as dt
import random

import db_ops
from user import User

bot = None
answerer = None
users = None

greetings_first = ["Hey!", "Hi!", "Good to see you!", "Nice to see you!", "It's nice to meet you!",
                   "Pleased to meet you!"]

greetings_next = ["Hello again!", "Good to see you again!", "How's it going?", "How are you doing?", "What's up?",
                  "Pleased to meet you again!"]

def on_inline_query(msg):
	def compute():
		query_id, from_id, query_string = telepot.glance(msg, flavor='inline_query')
		print ('Inline Query:', query_id, from_id, query_string)
		if query_string == "":
			query_string = "empty"

		articles = [InlineQueryResultArticle(
				id='abc',
				title=query_string,
				input_message_content=InputTextMessageContent(
				message_text=query_string
				)
				)]

		return articles

	answerer.answer(msg, compute)

def on_chosen_inline_result(msg):
	result_id, from_id, query_string = telepot.glance(msg, flavor='chosen_inline_result')
	print ('Chosen Inline Result:', result_id, from_id, query_string)

def on_chat_message(msg):
	content_type, chat_type, chat_id = telepot.glance(msg)
	flavor = telepot.flavor(msg)
	print "\n\nNew msg:\n"
	print "flavor:\t" + flavor + "\n"
	pprint(msg)

	if msg['chat']['type'] == "private" and content_type == "text":
		is_new_user = False
		if chat_id in users:
			user = users[chat_id]
		else:
			user = User(chat_id, msg['chat']['first_name'], 0, False, False)
			users[chat_id] = user
			is_new_user = True
		seen = dt.datetime.fromtimestamp(user.last_seen)
		elapsed_time = dt.datetime.now() - seen

		user.update_seen_time()

		msg_text = msg['text'].split()
		if msg_text[-1].lower() == "updates":
			user.want_time = msg_text[1].lower() == "on"
		elif msg_text[-1].lower() == "notifications":
			user.muted = msg_text[1].lower() == "off"
		elif is_new_user or elapsed_time.seconds > 60 * 60:
			greeting = random.choice(greetings_first if is_new_user else greetings_next)
			bot.sendMessage(chat_id, user.name + "! " + greeting)
			if not is_new_user:
				bot.sendMessage(chat_id, "Haven't seen you for " + str(elapsed_time))

		switch_timer = "Switch {0} time updates".format("off" if user.want_time else "on")
		switch_sound = "Turn {0} notifications".format("on" if user.muted else "off")
		keyboard = ReplyKeyboardMarkup(keyboard=[
			[switch_sound, switch_timer]
		])
		bot.sendMessage(chat_id, str(user), reply_markup=keyboard)
		db_ops.update_user(user, is_new_one=is_new_user)




if __name__ == "__main__":
	token_f = open("_token")

	_token = token_f.readline()
	if _token[-1] == '\n':
		_token = _token[:-1]

	bot = telepot.Bot(_token)

	print bot.getMe()

	users = db_ops.fetch_users()

	answerer = telepot.helper.Answerer(bot)
	bot.message_loop({'chat': on_chat_message, 'inline_query': on_inline_query, 'chosen_inline_result':
		on_chosen_inline_result})

	print ('Listening 2 ...')
	while True:
		time.sleep(30)
		for id, user in users.iteritems():
			if user.want_time:
				print "\nsending time to {0} ...".format(id)
				bot.sendMessage(id, "Current time: <b>" + str(dt.datetime.now().time().strftime('%H:%M:%S')) + "</b>",
								disable_notification=user.muted, parse_mode="html")




