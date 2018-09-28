from telethon import TelegramClient, sync
from azure_parser import get_azure_data
from config import keys
import pymysql

# Обьект для работы с SQL
def sql_init():
	return pymysql.connect(keys['db_host'], keys['db_user'], keys['db_password'], keys['db_name'])

# Обьект для работы с Telegram
def telegram_init():
	client = TelegramClient(keys['user'], keys['ID'], keys['HASH'])
	return client.start()


def get_user_seen(uid):
	db = sql_init()
	cursor = db.cursor()
	cursor.execute("SELECT MAX(Date) FROM Messages WHERE FromID = %i" % (uid))
	lastdate = cursor.fetchone()
	cursor.execute("SELECT MIN(Date) FROM Messages WHERE FromID = %i" % (uid))
	firstdate = cursor.fetchone()
	return (lastdate[0], firstdate[0])


def get_row(row, table, uid=0):
	db = sql_init()
	cursor = db.cursor()
	if uid:
		cursor.execute("SELECT %s FROM %s WHERE ID == %s" % (row, table, uid))
	else:
		cursor.execute("SELECT %s FROM %s" % (row, table))
	rows = cursor.fetchall()
	res = list()
	for x in rows:
		res.append(x[0])
	return res


def get_userID_by_username(client, message):
	# Если есть упоминание по username, получаем ID
	for el in message:
		if '@' in el:
			try:
				return client.get_entity(el).id
			except:
				pass
	return

# Функция для работы с SQL
def sql_push(table, mid, col=0, data=0, tid='ID'):
	db = sql_init()
	cursor = db.cursor()
	# Если data строка, обновляем в базе
	if col and isinstance(data, str):
		if "'" in data: # На случай, если в сообщении есть кавычки
			query = ''' UPDATE %s SET %s = "%s" WHERE %s = %s''' % (table, col, data, tid, mid)
		else:
			query = ''' UPDATE %s SET %s = '%s' WHERE %s = %s''' % (table, col, data, tid, mid)
	elif col and data and isinstance(data, int):
		query = '''UPDATE %s SET %s = %i WHERE %s = %s''' % (table, col, data, tid, mid)
	# Если col и data равны нулю - такого ряда в базе еще нет создаем ряд
	elif not col and not data:
		query = '''INSERT INTO %s(%s) VALUES(%s)''' % (table, tid, mid)
	else:
		return
	try:
		cursor.execute(query)
		db.commit()
	except:
		db.rollback()


def parse_participants(client, group):
	users = client.iter_participants(group)
	table = 'ChatParticipants'
	uid = 'User_ID'
	participants = get_row(uid, table)
	i = 0
	for user in users:
		if not user.id in participants:
			sql_push(table, user.id, 0, 0, uid)
		dates = get_user_seen(user.id)
		# Если есть значение в кортеже, заносим в базу
		if dates[1]:
			sql_push(table, user.id, 'First_Date_Seen', dates[1], uid)
		if dates[0]:
			sql_push(table, user.id, 'Last_Date_Seen', dates[0], uid)
		print('\r%i' % i, end='')
		i += 1
	print()


def parse_messages(client, group, last=20):
	group_ent = client.get_entity(group) 	# получаем обьект группы
	posts = client.iter_messages(group_ent) 	# получаем итератор сообщений
	table = 'Messages'
	ids = get_row('ID', table) 		# получаем список пользователей в бд
	i = 1
	for message in posts:
		# получаем сообщения по порядку, от последнего
		mid = message.id
		if mid and message.message:
			if not mid in ids: # если пользователя нету, заносим ID в базу
				sql_push(table, mid)
			sql_push(table, mid, 'Date', message.date.strftime('%Y-%m-%d'))
			if message.from_id:
				sql_push(table, mid, 'FromID', message.from_id)
			if message.message:
				sql_push(table, mid, 'Message', message.message)
			if message.reply_to_msg_id:
				sql_push(table, mid, 'ReplyMessageID', message.reply_to_msg_id)
			if message.fwd_from:
				sql_push(table, mid, 'ForwardID', message.fwd_from.from_id)
			if message.views:
				sql_push(table, mid, 'ViewCount', message.views)
			if hasattr(message.media, 'webpage'):
				if hasattr(message.media.webpage, 'url'):
					sql_push(table, mid, 'Formatting', message.media.webpage.url)
			if hasattr(message, 'entities'):
				if message.entities and '@' in message.message:
					sql_push(table, mid, 'Formatting', get_userID_by_username(client, message.message.split(' ')))

			# Следующий блок кода предназначен для работы с azure api

			# azure_data = get_azure_language(message.message) # получаем данные
			# if azure_data[0]:
			# 	sql_push(table, mid, 'Language', azure_data[0])
			# sql_push(table, mid, 'sentiment', int(azure_data[1]))
			# if azure_data[2]:
			# 	sql_push(table, mid, 'key_phrases', azure_data[2])
			# if azure_data[3]:
			# 	sql_push(table, mid, 'entities', azure_data[3])
			# break
		if hasattr(message, 'action'):
			if message.action: # Системные сообщения (добавление пользователя, итп)
				sql_push(table, mid, 'ServiceAction', str(message.action))
		if i >= last:
			print('\r%i')
			break
		print('\r%i' % i, end='')
		i += 1