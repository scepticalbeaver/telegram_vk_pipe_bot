import collections
import datetime as dt
import logging
import logging.handlers
import os
import Queue
import random
import threading
import time

import sync_tg_bot
import sync_vk_app

MAX_SHORT_PERIOD_FAILS = 3
SHORT_PERIOD = dt.timedelta(seconds=30)

def start_pipe_watchdog(tg_token_path, vk_token_path, log_filename = ""):
	assert os.path.exists(vk_token_path), "The path to vk credentials is broken"
	assert os.path.exists(tg_token_path), "The path to Telegram credentials is broken"

	logging.basicConfig(format='%(asctime)s:%(levelname)s:%(name)s:%(message)s', level=logging.INFO)
	logging.getLogger('requests').setLevel(logging.WARNING)
	if log_filename:
		max_log_size_bytes = 50 * 1024 * 1024 # 50 Mb
		fhandler = logging.handlers.RotatingFileHandler(filename=log_filename,
				mode="w", maxBytes=max_log_size_bytes, backupCount=1)
		logging.getLogger().addHandler(fhandler)

	random.seed((dt.datetime.now() - dt.datetime.fromtimestamp(0)).seconds)
	stop_signals_q = Queue.Queue()

	def vk_process():
		with open(vk_token_path) as credits_f:
			app_id = credits_f.readline().replace('\n', '')
			token = credits_f.readline().replace('\n', '')
			vk_node = sync_vk_app.SyncVkNode(app_id, token)
			vk_node.start(stop_signals_q)

	def telegram_process():
		with open(tg_token_path) as token_f:
			_token = token_f.readline().replace('\n', '')
			bot = sync_tg_bot.SyncBot(_token)
			bot.start(stop_signals_q)

	last_fails = collections.deque([dt.datetime.fromtimestamp(0)] * MAX_SHORT_PERIOD_FAILS)

	while dt.datetime.now() > last_fails[-MAX_SHORT_PERIOD_FAILS] + SHORT_PERIOD:
		try:
			vk_thread = threading.Thread(target=vk_process)
			vk_thread.daemon = True
			tg_thread = threading.Thread(target=telegram_process)
			tg_thread.daemon = True

			vk_thread.start()
			time.sleep(2)
			tg_thread.start()
			while True:
				time.sleep(5)

		except BaseException as e:
			if isinstance(e, KeyboardInterrupt):
				logging.info("Session was interrupted by user. Sending stop-event... Please wait")
				stop_signals_q.put("stop")
				sleep_seconds = 5
				time.sleep(sleep_seconds)
				break
			logging.error("Unexpected exception. Message: %s", e.message)
			last_fails.append(dt.datetime.now())
			last_fails.popleft()
	logging.info("Exit")
