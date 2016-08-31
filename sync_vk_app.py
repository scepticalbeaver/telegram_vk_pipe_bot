import time
import logging
import subprocess
import sys

import vk


class SyncVkNode(object):
	def __init__(self, token):
		self.app_id = 5611494
		self._token = token
		session = vk.AuthSession(id, token, "vk" + verb_phrase + "!")
		session = vk.Session(token)
		api = vk.API(session, lang='en')
		api.friends.get(order="hints")

	def monitor(self):
		tracked_chats



if __name__ == "__main__":
	logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

	token_f = open("vk_credits")
	token = token_f.readline()
	token = token.replace('\n', '')


	#try:
	#	phrase = subprocess.check_output(["codec/codec", "vk_credits"])
	#	phrase = phrase.replace('\n', '')
	#except subprocess.CalledProcessError as e:
	#	logging.error("cannot open file with credits. Reason: %s", e.message)
	#	sys.exit()


	vk_node = SyncVkNode(token)

	try:
		while True:
			time.sleep(1)
	except KeyboardInterrupt as e:
		logging.info("Keyboard interrupt. Quitting...")