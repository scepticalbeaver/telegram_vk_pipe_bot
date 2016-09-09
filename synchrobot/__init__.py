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

MAX_LONG_PERIOD_FAILS = 3
LONG_PERIOD = dt.timedelta(minutes=10)
SHORT_PERIOD = dt.timedelta(seconds=30)
WATCHDOG_PROBE_PERIOD_SECONDS = 10
RECOVERY_TIME_BASE_SECONDS = 5
FORMAT = '%(asctime)s %(levelname)s [%(name)s] %(message)s'


def start_pipe_watchdog(tg_token_path, vk_token_path, log_filename = ""):
	assert os.path.exists(vk_token_path), "The path to vk credentials is broken"
	assert os.path.exists(tg_token_path), "The path to Telegram credentials is broken"

	logging.basicConfig(format=FORMAT, level=logging.INFO)
	logging.getLogger('requests').setLevel(logging.WARNING)
	if log_filename:
		max_log_size_bytes = 50 * 1024 * 1024 # 50 Mb
		fhandler = logging.handlers.RotatingFileHandler(filename=log_filename,
				mode="w", maxBytes=max_log_size_bytes, backupCount=1)
		file_formatter = logging.Formatter(FORMAT)
		fhandler.setFormatter(file_formatter)
		logging.getLogger().addHandler(fhandler)
		logging.info("-" * 60)

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

	last_fail_time = dt.datetime.fromtimestamp(0)
	vk_thread = None
	tg_thread = None
	recovery_time_seconds = RECOVERY_TIME_BASE_SECONDS
	long_period_fails_counter = 0
	try:
		while long_period_fails_counter < MAX_LONG_PERIOD_FAILS:
			if vk_thread is None or not vk_thread.isAlive():
				vk_thread = threading.Thread(target=vk_process)
				vk_thread.daemon = True
				logging.info("Starting up vk...")
				vk_thread.start()
			time.sleep(2)
			if tg_thread is None or not tg_thread.isAlive():
				tg_thread = threading.Thread(target=telegram_process)
				tg_thread.daemon = True
				logging.info("Starting up Telegram...")
				tg_thread.start()

			time.sleep(WATCHDOG_PROBE_PERIOD_SECONDS)

			if not vk_thread.isAlive() or not tg_thread.isAlive():
				logging.warning("Failure was detected. vk state: %d; tg state: %d",
						vk_thread.isAlive(), tg_thread.isAlive())
				time_since_prev_fail = dt.datetime.now() - last_fail_time
				last_fail_time = dt.datetime.now()
				time.sleep(recovery_time_seconds)
				if recovery_time_seconds > RECOVERY_TIME_BASE_SECONDS and time_since_prev_fail > LONG_PERIOD:
					long_period_fails_counter += 1
				if time_since_prev_fail.seconds > recovery_time_seconds + SHORT_PERIOD.seconds:
					recovery_time_seconds = RECOVERY_TIME_BASE_SECONDS
				else:
					recovery_time_seconds *= 2

	except BaseException as e:
			if isinstance(e, KeyboardInterrupt):
				sleep_seconds = 5
				stop_signals_q.put("stop")
				logging.info("Session was interrupted by user. Sending stop-event... Please wait %d seconds",
						sleep_seconds)
				time.sleep(sleep_seconds)
			else:
				logging.exception("Unexpected exception in watchdog: %s", e.message)
	logging.info("Full application exit")
