import argparse

import synchrobot

version = "0.1"

description_text = "The program establish a pipe between VK social network and Telegram. So a user from telegram " \
	"is capable to write a message to a VK user and vice versa. It also supports group conversations.\n" \
	"Additional functionality:\n" \
	"In Telegram:\tbot can send a random famous quote using inline query\n" \
	"In VK: an app logs a user activity whether is it online / offline / using mobile\n"

epilog = "Author: Ivan Senin (sde.ivan.senin at Google's mailbox)\n" \
		"License: MIT (http://opensource.org/licenses/MIT) "

parser = argparse.ArgumentParser(description=description_text, version=version, epilog=epilog)

parser.add_argument("tg_token_file", type=str,
		help="a path to the file with single row -- telegram bot token")

parser.add_argument("vk_token_file", type=str,
		help="a path to the file with two rows -- vk app id and it's token")

parser.add_argument("--log", dest="log_filename", type=str, default="", metavar="log_filename",
		help="logs filename. It uses only stdout if this arg is empty")


args = parser.parse_args()


print args.log_filename

synchrobot.start_pipe_watchdog(args.tg_token_file, args.vk_token_file, args.log_filename)







