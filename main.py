import sqlite3
import pandas as pd
import sys
import ddl as ddl

conn = sqlite3.connect('sber.db')
cursor = conn.cursor()
# считывание таблицы в базу и создаем их backup
def xlsx2sql(filePath,tableName,conn):
	df = pd.read_excel(filePath)
	df.to_sql(tableName, con=conn, if_exists='replace',index=False)
	df.to_excel('backup_'+filePath[5:],sheet_name='sheet_name1',index=None)
# экспорт из базы в excel
def sql2xlsx(filePath,columns,tableName,conn):
	df=pd.read_sql(f'select {columns} from {tableName}',con=conn)
	df.to_excel(filePath,sheet_name='sheet_name1', index=None)

xlsx2sql(f'data/transactions_{sys.argv[1]}.xlsx','table_trans',conn)
xlsx2sql(f'data/passports_blacklist_{sys.argv[1]}.xlsx','black_list',conn)

#_____________________________________________ 
# Заполнение таблицы clients
#_____________________________________________
def insertClients():
	cursor.execute('''
		insert into clients(
			client_id,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone
		)
		select distinct
			client_id,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone
		from clientsNew
	''')
	cursor.execute('''
		update clients
		set effective_to=datetime(current_timestamp,'-1 seconds')
		where client_id in (select client_id from clientsUpdate)
		and effective_to = datetime('2999-12-31 23:59:59');
	''')
	cursor.execute('''
		insert into clients(
			client_id,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone
		)
		select
			client_id,
			last_name,
			first_name,
			patronymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone
		from clientsUpdate
	''')
	cursor.execute('''
		update clients
		set effective_to=current_timestamp
		where client_id in (select client_id from clientsDeleted)
		and effective_to = datetime('2999-12-31 23:59:59');
	''')
	cursor.execute('''
		update clients
		set deleted_flg=1
		where client_id in (select client_id from clientsDeleted)
		and effective_to = datetime('2999-12-31 23:59:59');
	''')
	conn.commit()

#_____________________________________________ 
# Заполнение таблицы accounts
#_____________________________________________
def InsertAccounts():
	cursor.execute('''
		insert into accounts(
			account_num,
			valid_to,
			client
		)
		select distinct
			account_num,
			valid_to,
			client
		from accountsNew
	''')
	cursor.execute('''
		update accounts
		set effective_to=datetime(current_timestamp,'-1 seconds') 
		where client in (select client from accountsUpdate)
		and effective_to = datetime('2999-12-31 23:59:59');
	''')
	cursor.execute('''
		insert into accounts(
			account_num,
			valid_to,
			client
		)
		select
			account_num,
			valid_to,
			client
		from accountsUpdate
	''')
	cursor.execute('''
		update accounts
		set effective_to=datetime(current_timestamp,'-1 seconds')
		where client in (select client from accountsDeleted)
		and effective_to = datetime('2999-12-31 23:59:59');
	''')
	cursor.execute('''
		update accounts
		set deleted_flg=1
		where client in (select client from accountsDeleted)
		and effective_to = datetime('2999-12-31 23:59:59');
	''')
	conn.commit()

#_____________________________________________ 
# Заполнение таблицы cards
#_____________________________________________
def insertCards():
	cursor.execute('''
		insert into cards(
			card_num,
			account_num
		)
		select distinct
			card_num,
			account_num
		from cardsNew
	''')

	cursor.execute('''
		update cards
		set effective_to=datetime(current_timestamp,'-1 seconds')
		where account_num in (select account_num from cardsUpdate)
		and effective_to = datetime('2999-12-31 23:59:59');
	''')

	cursor.execute('''
		insert into cards(
			card_num,
			account_num
		)
		select
			card_num,
			account_num
		from cardsUpdate
	''')
	cursor.execute('''
		update cards
		set effective_to=datetime(current_timestamp,'-1 seconds')
		where account_num in (select account_num from cardsDeleted)
		and effective_to = datetime('2999-12-31 23:59:59');
	''')
	cursor.execute('''
		update cards
		set deleted_flg=1
		where account_num in (select account_num from cardsDeleted)
		and effective_to = datetime('2999-12-31 23:59:59');
	''')
	conn.commit()
#_____________________________________________ 
# Заполнение таблицы passport_blacklist
#_____________________________________________
def insertBlackList():
	cursor.execute('''
		insert into passport_blacklist 
			(passport_num,
			entry_dt)
			select
				t1.passport,
				t1.start_dt
			from black_list t1
			left join passport_blacklist t2 on t1.passport=t2.passport_num
			where t2.passport_num is null
	''')
	conn.commit()
#_____________________________________________ 
# Заполнение таблицы report из таблицы fraud
#_____________________________________________
def insertReport():
	cursor.execute('''
		insert into report
			(event_dt, 
			passport,
			fio,
			phone,
			event_type,
			report_dt)
			select
				event_dt,
				passport_num,
				fio,
				phone,
				event_type,
				report_dt
			from fraud
			where event_dt in (select max(event_dt) from fraud group by passport_num)
	''')
	conn.commit()
#_____________________________________________ 
# Добавление новых паспортов в passport_blacklist
# из отчета report
#_____________________________________________
def insertBlackListNew():
	cursor.execute('''
		insert into passport_blacklist
			(passport_num,
			entry_dt)
			select
				t1.passport,
				t1.report_dt
			from report t1
			left join passport_blacklist t2 on t1.passport=t2.passport_num
			where t2.passport_num is null
	''')
	conn.commit()

# чистим временные таблицы
ddl.clearDB()
# создаем и актуализируем таблицу clietns
ddl.createTableClients()
ddl.createTableClients00()
ddl.createTableClientsNew()
ddl.createTableClientsUpdate()
ddl.createTableClientsDeleted()
insertClients()

# создаем и актуализируем таблицу accounts
ddl.createTableAccounts()
ddl.createTableAccounts00()
ddl.createTableAccountsNew()
ddl.createTableAccountsUpdate()
ddl.createTableAccountsDeleted()
InsertAccounts()

# создаем и актуализируем таблицу cards
ddl.createTableCards()
ddl.createTableCards00()
ddl.createTableCardsNew()
ddl.createTableCardsUpdate()
ddl.createTableCardsDeleted()
insertCards()


# создаем таблицы transactions, terminals
ddl.createTableTransactions()
ddl.createTableTerminals()

# создаем и актуализируем таблицу passport_blacklist
ddl.createTableBlackList()
insertBlackList()


# удаляем временный отчет fraud если он был
cursor.execute('drop table if exists fraud')	
# создаем и актуализируем report
ddl.createTableReport()
ddl.createTableFraud()
insertReport()


# добавляем найденых мошенников в наш список passport_blacklist
insertBlackListNew()


sql2xlsx('report.xlsx','*','report',conn)
