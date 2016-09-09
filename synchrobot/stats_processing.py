# -*- coding: utf-8 -*-
# Author: Ivan Senin

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os
from scipy.interpolate import spline
import threading

import synchrobot
import synchrobot.chat_user


def get_filename(user):
	DIR_NAME="stats_tmp"

	filename = "stats_"
	if isinstance(user, synchrobot.chat_user.User):
		filename += "{0}_".format( user.username if user.username else "id" + str(user.id))
	filename += threading.currentThread().getName() + ".png"

	if not os.path.exists(DIR_NAME):
		os.mkdir(DIR_NAME)
	return os.path.join(os.path.join(os.curdir, DIR_NAME), filename)

def make_attendance_plot(stats, user=None):
	HOURS = 24
	'''
	:param stats: numpy.matrix with columns (datetime, was_online (bool), via mobile (bool))
	:return: image
	'''
	assert isinstance(stats, np.ndarray)
	assert user is None or isinstance(user, synchrobot.chat_user.User)
	subject_name = user.username if user else "any"
	x_axis = range(HOURS)
	online_per_hours = [[] for _ in range(HOURS)]
	mobiles_per_hours = [[] for _ in range(HOURS)]
	days_per_hours = [[] for _ in range(HOURS)]
	for row in stats:
		online_per_hours[row[0].hour].append(row[1])
		mobiles_per_hours[row[0].hour].append(row[2])
		days_per_hours[row[0].hour].append(row[0].date())

	replace_nan = lambda x: 0 if x == np.nan else x
	take_avg = lambda observations: replace_nan(np.average( np.array(observations, np.float)))
	y_axis = map(take_avg, online_per_hours)
	y_mobile_axis = map(take_avg, mobiles_per_hours)

	x_microticks = np.linspace(0, HOURS, HOURS*100)
	y_mobile_s_axis = spline(x_axis, y_mobile_axis, x_microticks)

	days_per_hours = map(np.unique, days_per_hours)

	plt.figure()
	plt.title("Probability density of `online` status for {0}".format(subject_name.encode('utf-8')))
	plt.bar(x_axis, y_axis, align="center", color="cyan", hold=True, label="online")
	plt.plot(x_microticks, y_mobile_s_axis, '-', label="via mobile", linewidth=2.)
	plt.legend()
	plt.xlim(-0.1, 23.9)
	plt.ylim(-0.01, 1.19)
	plt.xlabel("Hours,\nTotal Number of Measurements,\nNumber of Distinct Days in View")
	plt.ylabel("Probability of Appearance")
	plt.grid(True)
	plt.xticks(range(HOURS), ["{0}\n{1}\n{2}".format(i, len(online_per_hours[i]),
			len(days_per_hours[i])) for i in range(HOURS)])
	plt.gcf().subplots_adjust(bottom=0.2)

	filename = get_filename(user)
	plt.savefig(filename)
	return filename








