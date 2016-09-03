#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime as dt
import logging
import requests
import threading
import time
import Queue

import vk_requests
import vk_requests.exceptions
from vk_requests.auth import VKSession

import db_ops


class SyncVkNode(object):
	NEW_MESSAGE = 4

	def __init__(self, app_id, token):
		self.app_id = 5611494
		self.__token = token

		session = VKSession(app_id=app_id)
		session.access_token = token
		self._api = vk_requests.API(session)
		self._api.friends.get()  # test
		logging.info("vk connection established")
		self.cached_users_d = {}

		self.db_client = db_ops.DBClient("vk")
		self.chats_to_monitor = self.db_client.get_monitored_chats()
		logging.info("%d chatd_ids to monitor were fetched from db", len(self.chats_to_monitor))
		self.msg_queue = Queue.Queue(100)
		self.outbox_msg_ids = []
		self.outbox_msg_ids_mx = threading.Lock()

	def _start_longpoll_handler(self):
		INVALID_VERSION = 4
		SLEEP_SECONDS = 1
		key = None
		server = None
		ts = None
		while True:
			if key is None:
				try:
					logging.info("Getting new keys for a long-poll...")
					res_d = self._api.messages.getLongPollServer(need_pts=0)
					server = res_d['server']
					key = res_d['key']
					ts = res_d['ts']
				except Exception as e:
					logging.warning("Unable to get new long-poll keys. Reason: %s", e.message)
					time.sleep(3)

			url = "https://{0}?act=a_check&key={1}&ts={2}&wait=25&mode=2".format(server, key, ts)
			req = requests.get(url)
			answer = req.json()
			if 'failed' in answer:
				if answer['failed'] == INVALID_VERSION:
					logging.error("Unexpected error. longpoll retured fail-4")
					raise ValueError("LongPoll resulted in FAIL-4")
				key = None  # forces to get new keys
				continue
			ts = answer['ts']
			updates = answer['updates']
			logging.info("Updates: %s", str(updates))

			for update in updates:
				if update[0] == self.NEW_MESSAGE:
					msg_d = {'message_id': update[1],
						'flags': update[2],
						'from_id': update[3],
						'timestamp': update[4],
						'text': update[6]}
					if msg_d['from_id'] in self.chats_to_monitor:
						self.msg_queue.put(msg_d)

			time.sleep(SLEEP_SECONDS)

	def __event_loop(self):
		logging.info("Starting event loop")
		new_msg_handler = ChatHandler(self.db_client, self._api, self.msg_queue, self.cached_users_d,
				self.outbox_msg_ids, self.outbox_msg_ids_mx)
		foreign_msg_handler = UnsyncMessagesHandler(self.db_client, self._api, self.outbox_msg_ids,
				self.outbox_msg_ids_mx)

		try:
			sleep_seconds = 1
			while True:
				new_msg_handler()
				foreign_msg_handler()

				time.sleep(sleep_seconds)
		except KeyboardInterrupt:
			logging.info("Event loop was interrupted by user")

	def start(self):
		collector_thread = threading.Thread(target=self._start_longpoll_handler)
		collector_thread.daemon = True
		collector_thread.start()

		self.__event_loop()


class ChatHandler(db_ops.Handler):
	MAX_CACHED_USERS = 500

	def __init__(self, db_client, api, msg_queue, cached_users, outbox_msg_ids, outbox_msg_ids_mx):
		super(ChatHandler, self).__init__()
		self.period = dt.timedelta(seconds=7)
		self.db_client = db_client
		self.api = api
		self.msg_q = msg_queue
		self.cached_users_d = cached_users
		self.outbox_msg_ids = outbox_msg_ids
		self.outbox_msg_ids_mx = outbox_msg_ids_mx

	def get_users_by_id(self, ids):
		if not ids:
			return
		users_info = self.api.users.get(user_ids=filter(lambda id: id not in self.cached_users_d.keys(), ids),
				fields='domain')
		logging.info("ChatHandler: fetchied users' information: %s", str(users_info))
		for user_info in users_info:
			self.cached_users_d[user_info['id']] = {'fname' : user_info['first_name'], 'username' : user_info['domain']}

	def find_sender_for_group_msgs(self, group_msgs):
		if not group_msgs:
			return
		full_msgs_info = self.api.messages.getById(
				message_ids=[msg['message_id'] for msg in group_msgs], preview_length=1)['items']
		logging.info("Requested msgs from group chats %s", str(full_msgs_info))
		for msg in group_msgs:
			msg['user_id'] = (filter(lambda msg_: msg_['id'] == msg['message_id'], full_msgs_info)[0])['user_id']

	def handler_hook(self, **kwargs):
		GROUP_IDS = 2000000000

		counter = 20
		messages = []
		while not self.msg_q.empty() and counter > 0:
			counter -= 1
			msg = self.msg_q.get()
			if msg['message_id'] in self.outbox_msg_ids:
				with self.outbox_msg_ids_mx:
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
			logging.error("Cannot get additional information about group messages. Reason: %s", e.message)
			# saving group messages back to queue. Hope to successfully save them within next iteration
			for g_msg in group_msgs:
				self.msg_q.put(g_msg)
				messages.remove(g_msg)

		for msg in messages:
			logging.info("ChatHandler: flushing to db msg: %s", str(msg))
			self.db_client.add_msg(
					msg_id=msg['message_id'],
					chat_id=msg['from_id'],
					sender_id=msg['user_id'],
					sender_name=self.cached_users_d[msg['user_id']]['fname'],
					username=self.cached_users_d[msg['user_id']]['username'],
					msg_type="text",
					content=msg['text'],
					date=msg['timestamp'])
		if len(self.cached_users_d.keys()) > self.MAX_CACHED_USERS:
			self.cached_users_d = {}


class UnsyncMessagesHandler(db_ops.Handler):
	def __init__(self, db_client, api, outbox_msg_ids, outbox_msg_ids_mx):
		super(UnsyncMessagesHandler, self).__init__()
		self.period = dt.timedelta(seconds=10)
		self.db_client = db_client
		self.api = api
		self.outbox_msg_ids = outbox_msg_ids
		self.outbox_msg_ids_mx = outbox_msg_ids_mx

	def handler_hook(self, **kwargs):
		for row_dict in self.db_client.fetch_unsync_messages():
			logging.info("Sending unsync message: %s ", str(row_dict))
			msg_time = dt.datetime.fromtimestamp(row_dict["date"]).strftime('%H:%M:%S')
			msg_text = "{0} ({1}), {2}: {3}".format(row_dict["sender_name"].encode('utf-8'),
					row_dict["username"].encode('utf-8'), msg_time, row_dict["content"].encode('utf-8'))
			target_chat = row_dict["vk_chat_id"]
			new_msg_id = self.api.messages.send(peer_id=target_chat, chat_id=target_chat, message=msg_text)
			with self.outbox_msg_ids_mx:
				self.outbox_msg_ids.append(new_msg_id)


if __name__ == "__main__":
	logging.basicConfig(format='vk side: %(asctime)s %(message)s', level=logging.DEBUG)

	credits_f = open("vk_credits")
	app_id = credits_f.readline().replace('\n', '')
	token = credits_f.readline().replace('\n', '')

	vk_node = SyncVkNode(app_id, token)

	vk_node.start()