from telethon import TelegramClient, sync
from azure_parser import get_azure_data
from datetime import datetime, timedelta
from config import keys
import pymysql


class Parsing(object):
	def __init__(self, group_name):
		self.db = pymysql.connect(keys['db_host'], keys['db_user'], keys['db_password'], keys['db_name'])
		self.cursor = self.db.cursor()
		self.client = TelegramClient(keys['user'], keys['ID'], keys['HASH']).start()
		self.group_ent = self.client.get_entity(group_name)


	def __get_user_seen(self, table, part_table, uid):
		self.cursor.execute("SELECT MIN(Date), MAX(Date) FROM %s WHERE FromID = %i" % (table, uid))
		m_dates = self.cursor.fetchall()
		self.cursor.execute("SELECT First_Date_Seen, Last_Date_Seen FROM %s WHERE User_ID = %i" 
			% (part_table, uid))
		c_dates = self.cursor.fetchall()
		return (m_dates, c_dates)


	def __int(self, fl):
		return 1 if fl >= 0.5 else 0


	def __s_min(self, val1, val2):
		if val1 and val2:
			return val1 if val1 < val2 else val2
		else:
			if val1:
				return val1
			elif val2:
				return val2


	def __table_exist(self, table):
		try:
			self.cursor.execute("SELECT 1 FROM %s LIMIT 1;" % (table))
			return True
		except:
			return False


	def __get_forward_table(self):
		if hasattr(self.group_ent, 'username'):
			tablename = self.group_ent.username + '_Forward'
		elif hasattr(self.group_ent, 'id'):
			tablename = self.group_ent.id + '_Forward'
		else:
			print("group Error")
			return False
		if not self.__table_exist(tablename):
			self.cursor.execute(
				''' CREATE TABLE %s (
				    ID INT,
				    OriginalDate DATE,
				    FromID INT,
				    ChannelPost INT
				)''' % (tablename)
			)
			print("%s table created" % tablename)
		return tablename


	def __get_participants_table(self):
		if hasattr(self.group_ent, 'username'):
			tablename = self.group_ent.username + '_ChatParticipants'
		elif hasattr(self.group_ent, 'id'):
			tablename = self.group_ent.id + '_ChatParticipants'
		else:
			print("group Error")
			return False
		if not self.__table_exist(tablename):
			self.cursor.execute(
				''' CREATE TABLE %s (
				    User_ID INT,
				    First_Date_Seen DATE,
				    Last_Date_Seen DATE
				);''' % (tablename)
			)
			print("%s table created" % tablename)
		return tablename


	def __get_messages_table(self):
		if hasattr(self.group_ent, 'username'):
			tablename = self.group_ent.username + '_Messages'
		elif hasattr(self.group_ent, 'id'):
			tablename = self.group_ent.id + '_Messages'
		else:
			print("group Error")
			return False
		if not self.__table_exist(tablename):
			self.cursor.execute(
				''' CREATE TABLE %s (
				    ID INT,
				    Date DATE,
				    FromID INT,
				    Message TEXT,
				    ReplyMessageID INT,
				    ViewCount INT,
				    Formatting TEXT,
				    ServiceAction TEXT,
				    Language TEXT,
				    key_phrases TEXT,
				    entities TEXT,
				    sentiment INT
				);''' % (tablename)
			)
			print("%s table created" % tablename)
		return tablename


	def __get_row(self, row, table, uid=0):
		if uid:
			self.cursor.execute("SELECT %s FROM %s WHERE ID == %s" % (row, table, uid))
		else:
			self.cursor.execute("SELECT %s FROM %s" % (row, table))
		rows = self.cursor.fetchall()
		res = list()
		for el in rows:
			res.append(el[0])
		return res


	def __get_userID_by_username(self, message):
		for el in message:
			if '@' in el:
				try:
					return self.client.get_entity(el).id
				except:
					pass
		return


	def get_datum_range(self, date_from, date_until):
		if hasattr(self.group_ent, 'username'):
			table = self.group_ent.username + '_Messages'
		elif hasattr(self.group_ent, 'id'):
			table = self.group_ent.id + '_Messages'
		else:
			print("group Error")
			return False
		self.cursor.execute("SELECT count(ID) from %s WHERE date >= '%s' and date <= '%s'" 
			% (table, date_from, date_until))
		rows = self.cursor.fetchone()
		return (rows[0], table)


	def get_message_range(self, date_from, date_until, table):
		self.cursor.execute("SELECT ID, message from %s WHERE date >= '%s' and date <= '%s'" 
			% (table, date_from, date_until))
		rows = self.cursor.fetchall()
		for elem in rows:
			azure_data = get_azure_data(elem[1])
			if azure_data[0]:
				self.__sql_push(table, elem[0], 'Language', azure_data[0])
			self.__sql_push(table, elem[0], 'sentiment', int(azure_data[1]))
			if azure_data[2]:
				self.__sql_push(table, elem[0], 'key_phrases', azure_data[2])
			if azure_data[3]:
				self.__sql_push(table, elem[0], 'entities', azure_data[3])


	def __sql_push(self, table, mid, col=0, data=-1, tid='ID'):
		if col and isinstance(data, str):
			if "'" in data:
				query = ''' UPDATE %s SET %s = "%s" WHERE %s = %s''' % (table, col, data, tid, mid)
			else:
				query = ''' UPDATE %s SET %s = '%s' WHERE %s = %s''' % (table, col, data, tid, mid)
		elif col and isinstance(data, int):
			if data >= 0:
				query = '''UPDATE %s SET %s = %i WHERE %s = %s''' % (table, col, data, tid, mid)
		elif not col and data == -1:
			query = '''INSERT INTO %s(%s) VALUES(%s)''' % (table, tid, mid)
		else:
			return
		try:
			self.cursor.execute(query)
			self.db.commit()
		except:
			self.db.rollback()


	def finish(self):
		self.db.close()


	def parse_participants(self):
		users = self.client.iter_participants(self.group_ent)
		new_users = self.client.iter_participants(self.group_ent)
		table = self.__get_participants_table()
		mess_table = self.__get_messages_table()
		uid = 'User_ID'
		participants = self.__get_row(uid, table)
		sql = self.__sql_push
		for user in new_users:
			active_user = user.id in participants
			if not active_user:
				if not participants:
					sql(table, user.id, 0, -1, uid)
				else:
					sql(table, user.id, 0, -1, uid)
		i = 0
		participants = self.__get_row(uid, table)
		for user in users:
			active_user = user.id in participants
			date = self.__get_user_seen(mess_table, table, user.id)
			if active_user:
				if date[0][0][0] or date[1][0][0]:
					sql(table, user.id, 'First_Date_Seen', self.__s_min(date[0][0][0],
						date[1][0][0]).strftime('%Y-%m-%d'), uid)
				else:
					sql(table, user.id, 'First_Date_Seen', datetime.now().strftime('%Y-%m-%d'), uid)
				sql(table, user.id, 'Last_Date_Seen', datetime.now().strftime('%Y-%m-%d'), uid)
			print('\r%i' % i, end='')
			i += 1
		print()


	def parse_messages(self, mod=0):
		posts = self.client.iter_messages(self.group_ent)
		table = self.__get_messages_table()
		forward_table = self.__get_forward_table()
		ids = self.__get_row('ID', table)
		sql = self.__sql_push
		if not table:
			return
		if isinstance(mod, int) and mod:
			last = True
		else:
			last = False
		if isinstance(mod, list):
			fdate = mod[0] + timedelta(hours=0)
			ldate = mod[1] + timedelta(hours=24)
			flag = True
		else:
			flag = False
		i = 1
		for message in posts:
			nf = True
			mid = message.id
			if flag:
				mdate = message.date.replace(tzinfo=None)
				nf = True if mdate >= fdate and mdate <= ldate else False
				if mdate < fdate:
					print()
					break
			if mid and nf:
				if not mid in ids:
					sql(table, mid)
				sql(table, mid, 'Date', message.date.strftime('%Y-%m-%d'))
				if hasattr(message, 'action'):
					if message.action:
						sql(table, mid, 'ServiceAction', str(message.action))
				if message.from_id:
					sql(table, mid, 'FromID', message.from_id)
				if hasattr(message, 'message'):
					if message.message:
						sql(table, mid, 'Message', message.message)
					if message.reply_to_msg_id:
						sql(table, mid, 'ReplyMessageID', message.reply_to_msg_id)
					if message.fwd_from:
						sql(forward_table, mid)
						sql(forward_table, mid, 'OriginalDate', message.date.strftime('%Y-%m-%d'))
						sql(forward_table, mid, 'FromID', message.fwd_from.from_id)
						sql(forward_table, mid, 'ChannelPost', self.group_ent.id)
						sql(table, mid, 'ForwardID', message.fwd_from.from_id)
					if message.views:
						sql(table, mid, 'ViewCount', message.views)
					if hasattr(message.media, 'webpage'):
						if hasattr(message.media.webpage, 'url'):
							sql(table, mid, 'Formatting', message.media.webpage.url)
					if hasattr(message, 'entities'):
						if message.entities and '@' in message.message:
							sql(table, mid, 'Formatting',
								self.__get_userID_by_username(message.message.split(' ')))

				# azure_data = get_azure_data(message.message)
				# print(azure_data)
				# if azure_data[0]:
				# 	sql(table, mid, 'Language', azure_data[0])
				# sql(table, mid, 'sentiment', self.__int(azure_data[1]))
				# if azure_data[2]:
				# 	sql(table, mid, 'key_phrases', azure_data[2])
				# if azure_data[3]:
				# 	sql(table, mid, 'entities', azure_data[3])

			if last:
				if i >= mod:
					print('\r%i' % i)
					break
			print('\r%i' % i, end='')
			i += 1