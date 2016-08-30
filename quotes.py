import os
import random
import calendar
import time

Q_PATH = "quotes"

random.seed(calendar.timegm(time.gmtime()))

def get_quote(subject = "random"):
	if not isinstance(subject, str):
		raise Exception ("subject must be a string")

	directory = os.path.join(os.path.curdir, Q_PATH)

	f = open(os.path.join(directory, subject + ".txt"))
	lines = f.readlines()
	f.close()
	return random.choice(lines) + " \ "