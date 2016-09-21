# Telegram <-> Vk pipe
The primary aim is to commute people, who prefer to chat within a specific platform.
A pipe could be established between group chats and private chats as well.

Telegram bot handles all the control. Ask him for [`/help`](tg://bot_command?command=help) for further info. 

Also, telegram and vk sides both have additional functionality.
Telegram bot is capable to process inline queries. That way you could send into an arbitraty chat a random famous quote via [@Synchrobot](https://web.telegram.org/#/im?p=%40synchrobot).

On the other hand, vk side is able to track users' online stats. Ontain stats by sending `/stats` or `/stats username` to the vk client.

## Usage
```
pipe.py [-h] [-v] [--log log_filename] tg_token_file vk_token_file

positional arguments:
  tg_token_file       a path to the file with single row -- telegram bot token
  vk_token_file       a path to the file with two rows -- vk app id and it's
                      token

optional arguments:
  -h, --help          show this help message and exit
  -v, --version       show program's version number and exit
  --log log_filename  logs filename. It uses only stdout if this arg is empty
```

## Implementation notes

All messages are dumped into sqlite db by client and then pulled by other side client. The implementation is based on standart sql syntax which allows easily migrate into a solid client-server DBMS. 

The application is highly fault tolerant and makes lot of attempts to restart in case of unexpected crash. Many server API errors are handled on a regular basis.

License: MIT (http://opensource.org/licenses/MIT)