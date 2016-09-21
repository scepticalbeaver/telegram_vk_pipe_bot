#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ivan Senin
import Queue
import datetime as dt
import logging
import requests
import threading
import time

import vk_requests
import vk_requests.exceptions
from vk_requests.auth import VKSession

from synchrobot import db_ops
from synchrobot.chat_user import User
import stats_processing

_WATCHES_FOR = "watches_for"

class SyncVkNode(object):
	NEW_MESSAGE_ID = 4

	def __init__(self, app_id, token):
		self.logger = logging.getLogger(__name__)
		self.app_id = app_id
		self.__token = token

		session = VKSession(app_id=app_id)
		session.access_token = token
		self._api = vk_requests.API(session)
		self._api.friends.get()  # test
		self.logger.info("vk connection established")

		self.db_client = db_ops.DBClient("vk")
		self.users_d = self.db_client.fetch_users()
		self.logger.info("%d users were fetched from db", len(self.users_d.keys()))
		self.users_d_mx = threading.Lock()

		self.chats_to_monitor = self.db_client.get_monitored_chats()
		self.logger.info("%d chatd_ids to monitor were fetched from db", len(self.chats_to_monitor))
		self.monitoring_mx = threading.Lock()

		self.msg_queue = Queue.Queue(100)
		self.new_users_q = Queue.Queue()
		self.outbox_msg_ids = []
		self.outbox_msg_ids_mx = threading.Lock()
		self.chats_to_activate_q = Queue.Queue()
		self.pending_chats_d = self.db_client.get_pending_chat_ids()
		self.request_for_stats_q = Queue.Queue()
		self.extend_vk_api()

	def extend_vk_api(self):
		self._api.fetch_users_from_web = self.fetch_users_from_web
		self._api.get_user_objects = self.get_user_objects

	def fetch_users_from_web(self, ids, overwrite_users=False):
		"""
		unsafe, exceptions could be thrown
		:return list if fetched users as `User` objects
		"""
		if not ids:
			return []
		if isinstance(ids, int) or isinstance(ids, basestring):
			ids = [ids]
		assert isinstance(ids, list) and (isinstance(ids[0], int) or isinstance(ids[0], basestring)), "Type error"
		self.logger.info("fetching users' information via web...")
		try:
			users_info = self._api.users.get(user_ids= ids,	fields='domain')
		except BaseException as e:
			self.logger.exception("Cannot fetch users from vk. Reason: %s", e.message)
			return

		result = []
		for user_info in users_info:
			id = user_info['id']
			if id in self.users_d.keys() and not overwrite_users:
				continue
			new_user = User(id, user_info['first_name'], 0, False, False, user_info['domain'])
			with self.users_d_mx:
				self.users_d[id] = new_user
			result.append(new_user)
			self.new_users_q.put(new_user)
		return result

	def get_user_objects(self, ids):
		"""
		:param ids: integer or username, can be list of it
		:return: chat_user.User object or list in unsorted order
		"""
		result = []
		unknown_ids = []
		if not ids:
			return result
		if isinstance(ids, int) or isinstance(ids, basestring):
			ids = [ids]
		for id in ids:
			if isinstance(id, int):
				if id in self.users_d.keys():
					result.append(self.users_d[id])
				else:
					unknown_ids.append(id)
			elif isinstance(ids, basestring):
				with self.users_d_mx:
					if id in [user.username for user in self.users_d.values()]:
						result.append( filter(lambda u: u.username == id, self.users_d.values())[0] )
					else:
						unknown_ids.append(id)
		result.extend(self.fetch_users_from_web(unknown_ids))
		res_size = len(result)
		if res_size != len(ids):
			self.logger.warning("Number of requested users ({0}) does not match the number of returned ones ({1})",
					len(ids), res_size)
		return result[0] if res_size == 1 else result

	def find_by_username(self, username):
		with self.users_d_mx:
			result = filter(lambda user: user.username == username, self.users_d.values())
		return result[0] if len(result) == 1 else result


	def on_chat_message(self, msg_d):
		GROUP_IDS = 2000000000
		words = [s.lower() for s in msg_d['text'].split()]
		is_private = msg_d['from_id'] < GROUP_IDS
		if "/stats" in words[:1] and msg_d['from_id'] > 0 and is_private:
			source = self.get_user_objects(msg_d['from_id'])
			target = source.id
			user = source
			if len(words) > 1:
				try:
					target = int(words[1])
					user = self.users_d[target] if target in self.users_d.keys() else None
				except:
					user = self.find_by_username(words[1])

			reply = ""
			now = dt.datetime.now()
			dt_relax = dt.timedelta(minutes=1)
			next_attempt_time = dt.datetime.fromtimestamp(source.last_seen) + dt_relax
			if not user:
				reply = "No statistics for user or broken name {0}.\n".format(words[1]) + \
						"Usage: /stats [id|username]\nwhere id is integer (e.g. `/stats 1` or `/stats durov`)" \
						"Type: `/stats` to watch statistics for yourself"
			elif now > next_attempt_time:
				self.request_for_stats_q.put((source, user))
				reply = "Assembling statistics for {0}...".format(now.isoformat('/'))
			elif source.want_time:
				seconds_rest = (next_attempt_time - now).seconds
				reply = "Please have a rest for {0} seconds".format(seconds_rest)
				source.want_time = False




			if reply:
				try:
					self._api.messages.send(peer_id=source.id, message=reply)
				except BaseException as e:
					self.logger.exception("Cannot send message to the user. Reason: %s", e.message)

	def _start_longpoll_handler(self):
		INVALID_VERSION = 4
		SLEEP_SECONDS = 1
		key = None
		server = None
		ts = None
		while True:
			if key is None:
				try:
					self.logger.info("Getting new keys for a long-poll...")
					res_d = self._api.messages.getLongPollServer(need_pts=0)
					server = res_d['server']
					key = res_d['key']
					ts = res_d['ts']
					self.logger.info("Got keys. Success")
				except BaseException as e:
					self.logger.exception("Unable to get new long-poll keys. Reason: %s", e.message)
					time.sleep(3)
					ontinue

			url = "https://{0}?act=a_check&key={1}&ts={2}&wait=25&mode=2".format(server, key, ts)
			try:
				req = requests.get(url)
				answer = req.json()
			except BaseException as e:
				self.logger.exception("Long-poll request failure. Reason: %s", e.message)
				time.sleep(SLEEP_SECONDS)
				continue

			if 'failed' in answer:
				self.logger.warning("Received `fail` from long-poll reply")
				if answer['failed'] == INVALID_VERSION:
					self.logger.error("Bad stuff: longpoll retured fail- %d", INVALID_VERSION)
					raise ValueError("LongPoll resulted in FAIL-4")
				key = None  # forces to get new keys
				continue
			ts = answer['ts']
			updates = answer['updates']

			for update in updates:
				if update[0] == self.NEW_MESSAGE_ID:
					msg_d = {'message_id': update[1],
						'flags': update[2],
						'from_id': update[3],
						'timestamp': update[4],
						'text': update[6],
						'attachments': update[7]}
					has_handled = False
					with self.monitoring_mx:
						if msg_d['from_id'] in self.chats_to_monitor:
							self.msg_queue.put(msg_d)
							has_handled = True
						elif msg_d['from_id'] in self.pending_chats_d.keys() and msg_d['text']:
							code = msg_d['text'].split()[0]
							if code == self.pending_chats_d[msg_d['from_id']]:
								self.logger.info("_start_longpoll_handler: found activation code match")
								self.chats_to_activate_q.put((msg_d['from_id'], code))
								has_handled = True
					if not has_handled:
						self.on_chat_message(msg_d)

			time.sleep(SLEEP_SECONDS)

	def __event_loop(self, stop_signal_q):
		collector_thread = None
		self.logger.info("Starting event loop")
		new_msg_handler = ChatHandler(self.db_client, self._api, self.msg_queue, self.users_d, self.outbox_msg_ids,
				self.outbox_msg_ids_mx)
		foreign_msg_handler = UnsyncMessagesHandler(self.db_client, self._api, self.outbox_msg_ids,
				self.outbox_msg_ids_mx)
		chats_state_handler = PipeUpdatesHandler(self.db_client, self.chats_to_activate_q, self._api)
		user_updates_handler = db_ops.UserUpdatesHandler(self.db_client, self.users_d)
		users_observer = UsersObservationHandler(self.db_client, self._api, self.users_d)
		statistics_processor = StatisticsProcessor(self.db_client, self._api, self.request_for_stats_q)

		try:
			sleep_seconds = 0.3
			while stop_signal_q.empty():
				res = chats_state_handler(outbox_msg_ids_mx=self.outbox_msg_ids_mx, outbox_msg_ids=self.outbox_msg_ids)
				if not res is None:
					self.chats_to_monitor, self.pending_chats_d = res
				new_msg_handler()
				foreign_msg_handler()
				users_observer(users_mx=self.users_d_mx)
				statistics_processor()
				user_updates_handler(users_mx=self.users_d_mx, new_users=self.new_users_q)

				time.sleep(sleep_seconds)
				if collector_thread is None or not collector_thread.isAlive():
					self.logger.info("Starting longpoll handler...")
					collector_thread = threading.Thread(target=self._start_longpoll_handler)
					collector_thread.daemon = True
					collector_thread.start()
		except KeyboardInterrupt:
			self.logger.info("Event loop was interrupted by user")

	def start(self, stop_signal_q = Queue.Queue()):
		self.__event_loop(stop_signal_q)
		self.db_client.close()
		if not stop_signal_q.empty():
			self.logger.info("Execution was stopped via stop-event")


class ChatHandler(db_ops.Handler):
	MAX_CACHED_USERS = 500

	def __init__(self, db_client, api, msg_queue, users_d, outbox_msg_ids, outbox_msg_ids_mx):
		super(ChatHandler, self).__init__(db_client, api)
		self.period = dt.timedelta(seconds=3)
		self.logger = logging.getLogger(__name__)
		self.msg_q = msg_queue
		self.users_d = users_d
		self.outbox_msg_ids = outbox_msg_ids
		self.outbox_msg_ids_mx = outbox_msg_ids_mx

	def find_sender_for_group_msgs(self, group_msgs):
		if not group_msgs:
			return
		full_msgs_info = self.api.messages.getById(
				message_ids=[msg['message_id'] for msg in group_msgs], preview_length=1)['items']
		self.logger.info("Requested msgs from group chats %s", str(full_msgs_info))
		for msg in group_msgs:
			msg['user_id'] = (filter(lambda msg_: msg_['id'] == msg['message_id'], full_msgs_info)[0])['user_id']

	def handler_hook(self, **kwargs):
		GROUP_IDS = 2000000000

		counter = 20
		messages = []
		while not self.msg_q.empty() and counter > 0:
			counter -= 1
			msg = self.msg_q.get()
			with self.outbox_msg_ids_mx:
				if msg['message_id'] in self.outbox_msg_ids:
					self.outbox_msg_ids.remove(msg['message_id'])
					continue
			messages.append(msg)

		group_msgs = []
		for msg in messages:
			if msg['from_id'] > GROUP_IDS:
				group_msgs.append(msg)
			else:
				msg['user_id'] = msg['from_id']

		try:
			self.find_sender_for_group_msgs(group_msgs)
			self.api.fetch_users_from_web([msg['user_id'] for msg in messages])
		except BaseException as e:
			self.logger.exception("Cannot get additional information about group messages. Reason: %s", e.message)
			# saving group messages back to queue. Hope to successfully save them within next iteration
			for g_msg in group_msgs:
				self.msg_q.put(g_msg)
				messages.remove(g_msg)

		for msg in messages:
			self.logger.info("ChatHandler: flushing to db msg: %s", str(msg))
			self.db_client.add_msg(
					msg_id=msg['message_id'],
					chat_id=msg['from_id'],
					sender_id=msg['user_id'],
					sender_name=self.users_d[msg['user_id']].name,
					username=self.users_d[msg['user_id']].username,
					msg_type="text",
					content=msg['text'],
					date=msg['timestamp'])


class UnsyncMessagesHandler(db_ops.Handler):
	def __init__(self, db_client, api, outbox_msg_ids, outbox_msg_ids_mx):
		super(UnsyncMessagesHandler, self).__init__(db_client, api)
		self.period = dt.timedelta(seconds=4)
		self.logger = logging.getLogger(__name__)
		self.outbox_msg_ids = outbox_msg_ids
		self.outbox_msg_ids_mx = outbox_msg_ids_mx
		self.send_counter = 3
		self.send_counter_mx = threading.Lock()

	def handler_hook(self, **kwargs):
		for row_dict in self.db_client.fetch_unsync_messages():
			with self.send_counter_mx:
				self.send_counter -= 1
			self.logger.info("Sending unsync message: %s ", str(row_dict))
			msg_time = dt.datetime.fromtimestamp(row_dict["date"]).strftime('%H:%M:%S')
			msg_text = "{0} ({1}), {2}: {3}".format(row_dict["sender_name"].encode('utf-8'),
					row_dict["username"].encode('utf-8'), msg_time, row_dict["content"].encode('utf-8'))
			target_chat = row_dict["vk_chat_id"]
			with self.send_counter_mx:
				if self.send_counter <= 0:
					time.sleep(1)
					self.send_counter = 3
				try:
					new_msg_id = self.api.messages.send(peer_id=target_chat,
							chat_id=target_chat, random_id=row_dict['date'], message=msg_text)
					with self.outbox_msg_ids_mx:
						self.outbox_msg_ids.append(new_msg_id)
					row_dict['sent'] = True
				except vk_requests.exceptions.VkAPIError as e:
					self.logger.error("UnsyncMessagesHandler: vk api error: %s; text: %s", e.message, msg_text)
					msg = e.message.split()
					if msg[0] == "Flood":
						self.api.messages.send(peer_id=target_chat, chat_id=target_chat,
								message="<Banned by flood control>")
				except BaseException as be:
					self.logger.exception("Unexpected exception: %s", be.message)


class PipeUpdatesHandler(db_ops.Handler):
	def __init__(self, db_client, chats_to_update_q, api):
		super(PipeUpdatesHandler, self).__init__(db_client, api)
		self.period = dt.timedelta(seconds=3)
		self.logger = logging.getLogger(__name__)
		self.chats_to_update_q = chats_to_update_q

	def handler_hook(self, **kwargs):
		while not self.chats_to_update_q.empty():
			vk_chat_id, code = self.chats_to_update_q.get()
			for row_dict in self.db_client.check_pending_chats(code):
				if row_dict['vk_chat_id'] == vk_chat_id:
					row_dict['confirmed'] = True
					self.logger.info("PipeUpdatesHandler: chat %d confirmed", vk_chat_id)
					outbox_msg_id = self.api.messages.send(peer_id=vk_chat_id, message="The pipe is confirmed")
					with kwargs['outbox_msg_ids_mx']:
						kwargs['outbox_msg_ids'].append(outbox_msg_id)

		chats_to_monitor = self.db_client.get_monitored_chats()
		pending_chats_d = self.db_client.get_pending_chat_ids()
		return chats_to_monitor, pending_chats_d


class UsersObservationHandler(db_ops.Handler):
	MINUTES_FRACTION = 10

	def __init__(self, db_client, api, users_d):
		super(UsersObservationHandler, self).__init__(db_client, api)
		self.logger = logging.getLogger(__name__)
		self.last_observation = dt.datetime.fromtimestamp(0)
		self.users_d = users_d

	def is_time_to_go(self, current_time):
		return 0 == current_time.minute % self.MINUTES_FRACTION and \
				current_time > self.last_observation + dt.timedelta(minutes=2)

	def handler_hook(self, **kwargs):
		users_mx = kwargs['users_mx']
		with users_mx:
			users_to_watch = map(lambda u: u.id, filter(lambda u: not u.muted, self.users_d.values()))

		try:
			users_info = self.api.users.get(user_ids=users_to_watch, fields="online")
		except BaseException as be:
			self.logger.exception("UsersObservationHandler: Unexpected exception: %s", be.message)
			self.logger.warning("Observation was skipped")
			return

		users_to_state_d = {}
		for j_user in users_info:
			id = j_user['id']
			user = self.users_d[id]
			is_online = j_user['online'] == 1
			using_mobile = "online_mobile" in j_user.keys()
			users_to_state_d[user] = (is_online, using_mobile)
		self.db_client.append_users_observations(users_to_state_d)
		self.last_observation = dt.datetime.now()

		# stats
		total_num = len(users_to_state_d.keys())
		online_num = len(filter(lambda pair: pair[0], users_to_state_d.values()))
		using_mobile = len(filter(lambda pair: pair[1], users_to_state_d.values()))
		mobile_fraction = float(using_mobile * 100) / online_num if online_num else 0
		self.logger.info("UsersObservationHandler: %d of %d are online. %.1f%s use mobile app", online_num, total_num,
				(mobile_fraction), "%")


class StatisticsProcessor(db_ops.Handler):
	RELAX_PERIOD = dt.timedelta(minutes=1)
	def __init__(self, db_client, api, pending_users_q):
		super(StatisticsProcessor, self).__init__(db_client, api)
		self.period = dt.timedelta(seconds=1)
		self.logger = logging.getLogger(__name__)
		self.pending_users_q = pending_users_q

	def upload_image(self, filename):
		import pprint as pp
		# step 1: Get server
		try:
			upload_server = self.api.photos.getMessagesUploadServer()
			assert 'upload_url' in upload_server
		except BaseException as e:
			self.logger.error("Cannot get photos' servername. Reason: %s", e.message)
			return
		# step 2: post image
		image = {'photo': open(filename, 'rb')}
		try:
			req = requests.post(url=upload_server['upload_url'], files=image)
			answer = req.json()
		except BaseException as e:
			self.logger.error("Cannot upload image. Reason: %s", e.message)
			return
		ans_photo_decoded = answer['photo'].decode('string-escape')
		# step 3: save image
		try:
			image_d = self.api.photos.saveMessagesPhoto(photo=ans_photo_decoded, server=answer['server'],
					hash=answer['hash'])
		except BaseException as e:
			self.logger.error("Cannot save uploaded photo. Reason: %s", e.message)
			return
		return image_d[0]


	def handler_hook(self, **kwargs):
		if self.pending_users_q.empty():
			return
		client_user, target_user = self.pending_users_q.get()
		if dt.datetime.now() < dt.datetime.fromtimestamp(client_user.last_seen) + self.RELAX_PERIOD:
			return
		try:
			self.api.messages.setActivity(user_id=target_user.id, type="typing", peer_id=client_user.id)
		except BaseException as e:
			self.logger.warning("Cannot set typing activity. Reason: %s", e.message)

		start_time = time.time()
		stats = self.db_client.get_user_statistics(target_user)
		if 0 == max(stats.shape):
			reply = "No statistics on user %s".format(str(target_user))
			self.api.messages.send(peer_id=client_user.id, message=reply)
		stats_filename = stats_processing.make_attendance_plot(stats, target_user)
		end_time = time.time()
		elapsed = end_time - start_time
		self.logger.info("Statistics plot was generated within %.2f seconds. File: %s", elapsed, stats_filename)

		# upload photo to vk
		start_time = time.time()
		image_d = self.upload_image(stats_filename)
		if not image_d:
			return
		end_time = time.time()
		elapsed = end_time - start_time
		self.logger.info("Image was uploaded to a server within %.2f seconds", elapsed)

		reply_text = "Statistics of {0}".format(target_user.username)
		payload = "photo{0}_{1}".format(image_d['owner_id'], image_d['id'])

		try:
			self.api.messages.send(peer_id=client_user.id, message=reply_text, attachment=payload)
		except BaseException as e:
			self.logger.error("Cannot send message with statistics. Reason: %s", e.message)
		client_user.update_seen_time()
		client_user.want_time = True


if __name__ == "__main__":
	logging.basicConfig(format='%(asctime)s:%(levelname)s:%(name)s:%(message)s', level=logging.INFO)
	logging.getLogger('requests').setLevel(logging.WARNING)

	credits_f = open("vk_credits")
	app_id = credits_f.readline().replace('\n', '')
	token = credits_f.readline().replace('\n', '')

	vk_node = SyncVkNode(app_id, token)

	vk_node.start()
