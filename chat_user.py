import calendar
import time
import datetime as dt

class User(object):
	def __init__(self, id, name, last_seen, want_time, muted):
		super(User, self).__init__()
		self.id = id
		self.name = name
		self.last_seen = last_seen
		self.want_time = want_time
		self.muted = muted

	def update_seen_time(self):
		self.last_seen = calendar.timegm(time.gmtime())

	def __str__(self):
		seen_str = dt.datetime.fromtimestamp(self.last_seen).strftime('%Y-%m-%d %H:%M:%S')

		return "User: ({0}, id: {1}, last_seen: {2}, want_time: {3}, muted: {4})".format(self.name.encode('utf-8'),
				self.id, seen_str, self.want_time, self.muted)


