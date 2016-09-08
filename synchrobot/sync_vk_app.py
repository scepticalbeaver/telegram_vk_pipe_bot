#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ivan Senin
import Queue
import datetime as dt
import logging
import random
import requests
import threading
import time

import vk_requests
import vk_requests.exceptions
from vk_requests.auth import VKSession

from synchrobot import db_ops
from synchrobot.chat_user import User


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
		self.users_d_mx = threading.Lock()
		self.chats_to_monitor = []
		self.logger.info("%d chatd_ids to monitor were fetched from db", len(self.chats_to_monitor))
		self.msg_queue = Queue.Queue(100)
		self.outbox_msg_ids = []
		self.outbox_msg_ids_mx = threading.Lock()
		self.monitoring_mx = threading.Lock()
		self.chats_to_activate_q = Queue.Queue()
		self.pending_chats_d = {}

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
				except Exception as e:
					self.logger.exception("Unable to get new long-poll keys. Reason: %s", e.message)
					time.sleep(3)

			url = "https://{0}?act=a_check&key={1}&ts={2}&wait=25&mode=2".format(server, key, ts)
			req = requests.get(url)
			answer = req.json()
			if 'failed' in answer:
				if answer['failed'] == INVALID_VERSION:
					self.logger.error("Unexpected error. longpoll retured fail-4")
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
						'text': update[6]}
					with self.monitoring_mx:
						if msg_d['from_id'] in self.chats_to_monitor:
							self.msg_queue.put(msg_d)
						elif msg_d['from_id'] in self.pending_chats_d.keys() and msg_d['text']:
							code = msg_d['text'].split()[0]
							if code == self.pending_chats_d[msg_d['from_id']]:
								self.logger.info("_start_longpoll_handler: found activation code match")
								self.chats_to_activate_q.put((msg_d['from_id'], code))

			time.sleep(SLEEP_SECONDS)

	def __event_loop(self, stop_signal_q):
		self.logger.info("Starting event loop")
		new_msg_handler = ChatHandler(self.db_client, self._api, self.msg_queue, self.users_d, self.users_d_mx,
				 self.outbox_msg_ids, self.outbox_msg_ids_mx)
		foreign_msg_handler = UnsyncMessagesHandler(self.db_client, self._api, self.outbox_msg_ids,
				self.outbox_msg_ids_mx)
		chats_state_handler = PipeUpdatesHandler(self.db_client, self.chats_to_activate_q, self._api)
		users_observer = UsersObservationHandle(self.db_client, self._api, self.users_d)

		try:
			sleep_seconds = 0.3
			while stop_signal_q.empty():
				res = chats_state_handler(outbox_msg_ids_mx=self.outbox_msg_ids_mx, outbox_msg_ids=self.outbox_msg_ids)
				if not res is None:
					self.chats_to_monitor, self.pending_chats_d = res
				new_msg_handler()
				foreign_msg_handler()
				users_observer(users_mx=self.users_d_mx)

				time.sleep(sleep_seconds)
		except KeyboardInterrupt:
			self.logger.info("Event loop was interrupted by user")

	def start(self, stop_signal_q = Queue.Queue()):
		collector_thread = threading.Thread(target=self._start_longpoll_handler)
		collector_thread.daemon = True
		collector_thread.start()

		self.__event_loop(stop_signal_q)
		self.db_client.close()
		if not stop_signal_q.empty():
			self.logger.info("Execution was stopped via stop-event")


class ChatHandler(db_ops.Handler):
	MAX_CACHED_USERS = 500

	def __init__(self, db_client, api, msg_queue, users_d, users_d_mx, outbox_msg_ids, outbox_msg_ids_mx):
		super(ChatHandler, self).__init__(db_client, api)
		self.period = dt.timedelta(seconds=3)
		self.logger = logging.getLogger(__name__)
		self.msg_q = msg_queue
		self.users_d = users_d
		self.users_d_mx = users_d_mx
		self.outbox_msg_ids = outbox_msg_ids
		self.outbox_msg_ids_mx = outbox_msg_ids_mx

	def get_users_by_id(self, ids):
		if not ids:
			return
		self.logger.info("ChatHandler: fetching users' information...")
		users_info = self.api.users.get(user_ids=filter(lambda id: id not in self.users_d.keys(), ids),
				fields='domain')

		new_users_l = []
		for user_info in users_info:
			id = user_info['id']
			new_user = User(id, user_info['first_name'], 0, False, False, user_info['domain'])
			with self.users_d_mx:
				self.users_d[id] = new_user
			new_users_l.append(new_user)
			self.logger.info("ChatHandler: flushing new user to db: (%s, %s)", new_user.username, new_user.name)
		self.db_client.update_user(new_users_l, is_new_ones=True)

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
			self.get_users_by_id([msg['user_id'] for msg in messages])
		except vk_requests.exceptions.VkAPIError as e:
			self.logger.error("Cannot get additional information about group messages. Reason: %s", e.message)
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


class UsersObservationHandle(db_ops.Handler):
	PERIOD_BASE_MINUTES = 5
	PERIOD_MAX_DELTA_MINUTES = 25

	def __init__(self, db_client, api, users_d):
		super(UsersObservationHandle, self).__init__(db_client, api)
		self.period = dt.timedelta(minutes=self.PERIOD_BASE_MINUTES)
		self.logger = logging.getLogger(__name__)
		self.users_d = users_d

	def update_handler_period(self):
		in_minutes = self.PERIOD_BASE_MINUTES + random.randint(0, self.PERIOD_MAX_DELTA_MINUTES)
		self.period = dt.timedelta(minutes=in_minutes)

	def handler_hook(self, **kwargs):
		friends_info = self.api.friends.get(fields="online, domain")['items']
		users_mx = kwargs['users_mx']
		users_to_state_d = {}
		new_users_l = []
		for j_user in friends_info:
			id = j_user['id']
			if id not in self.users_d: # new user
				user = User(id, j_user['first_name'], 0, False, False, j_user['domain'])
				user.dirty = False
				with users_mx:
					self.users_d[id] = user
				new_users_l.append(user)
				self.logger.info("ChatHandler: flushing new user to db: (%s, %s)", user.username, user.name)
			user = self.users_d[id]
			is_online = j_user['online'] == 1
			using_mobile = "online_mobile" in j_user.keys()
			users_to_state_d[user] = (is_online, using_mobile)
		self.db_client.update_user(new_users_l, is_new_ones=True)
		self.db_client.append_users_observations(users_to_state_d)

		self.update_handler_period()
		self.logger.info("UsersObservationHandle: next observation will be in %d minutes", self.period.seconds / 60)


if __name__ == "__main__":
	logging.basicConfig(format='%(asctime)s:%(levelname)s:%(name)s:%(message)s', level=logging.INFO)
	logging.getLogger('requests').setLevel(logging.WARNING)

	random.seed((dt.datetime.now() - dt.datetime.fromtimestamp(0)).seconds)

	credits_f = open("vk_credits")
	app_id = credits_f.readline().replace('\n', '')
	token = credits_f.readline().replace('\n', '')

	vk_node = SyncVkNode(app_id, token)

	vk_node.start()
