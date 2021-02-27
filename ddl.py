import sqlite3

conn = sqlite3.connect('sber.db')
cursor = conn.cursor()

#_____________________________________________ 
# Создание таблиц
#_____________________________________________
def createTableClients():
	cursor.execute('''
		CREATE table if not exists clients(
			client_id varchar(64),
			last_name varchar(64),
			first_name varchar(64),
			patronymic varchar(64),
			date_of_birth datetime,
			passport_num varchar(64),
			passport_valid_to datetime,
			phone varchar(64),
			effective_from datetime default current_timestamp,
			effective_to datetime default (datetime('2999-12-31 23:59:59')),
			deleted_flg char(1) default 0
			)
	''')
	cursor.execute('''
		CREATE view if not exists v_clients as 
			select distinct
				client_id,
				last_name,
				first_name,
				patronymic,
				date_of_birth,
				passport_num,
				passport_valid_to,
				phone
			from clients
			where current_timestamp between effective_from and effective_to
	''')

def createTableAccounts():
	cursor.execute('''
		CREATE table if not exists accounts (
			account_num varchar(64),
			valid_to datetime,
			client varchar(64),
			effective_from datetime default current_timestamp,
			effective_to datetime default (datetime('2999-12-31 23:59:59')),
			deleted_flg char(1) default 0
		)
	''')
	cursor.execute('''
		CREATE view if not exists v_accounts as
			select distinct
				account_num,
				valid_to,
				client
			from accounts
			where current_timestamp between effective_from and effective_to
	''')

def createTableCards():
	cursor.execute('''
		CREATE table if not exists cards (
			card_num varchar(64),
			account_num varchar(64),
			effective_from datetime default current_timestamp,
			effective_to datetime default (datetime('2999-12-31 23:59:59')),
			deleted_flg char(1) default 0		
		)
	''')
	cursor.execute('''
		CREATE view if not exists v_cards as
			select distinct
				card_num,
				account_num
			from cards
			where current_timestamp between effective_from and effective_to
	''')

def createTableTransactions():
	cursor.execute('''
		CREATE table if not exists transactions as 
			select
				trans_id,
				date as trans_date,
				card as card_num,
				oper_type,
				amount as amt,
				oper_result,
				terminal
			from table_trans
		''')

def createTableTerminals():
	cursor.execute('''
		CREATE table if not exists terminals as
			select
				terminal as terminal_id,
				terminal_type,
				city as terminal_city,
				address as terminal_address
			from table_trans
	''')

def createTableBlackList():
	cursor.execute('''
		CREATE table if not exists passport_blacklist(
			passport_num,
			entry_dt
			)
	''')

#_____________________________________________ 
# Создание временных таблиц New, Update, Deleted
# для Clients, Accounts, Cards
#_____________________________________________
def createTableClients00():
	cursor.execute('''
		create table clients_00 as
			select distinct
				client as client_id,
				last_name,
				first_name,
				patronymic,
				date_of_birth,
				passport as passport_num, 
				passport_valid_to,
				phone
			from table_trans
	''')

def createTableClientsNew():
	cursor.execute('''
		create table clientsNew as
			select
				t1.client_id,
				t1.last_name,
				t1.first_name,
				t1.patronymic,
				t1.date_of_birth,
				t1.passport_num,
				t1.passport_valid_to,
				t1.phone
			from clients_00 t1
			left join v_clients t2 on t1.client_id=t2.client_id
			where t2.client_id is null
	''')

def createTableClientsUpdate():
	cursor.execute('''
		create table clientsUpdate as
			select 
				t1.client_id,
				t1.last_name,
				t1.first_name,
				t1.patronymic,
				t1.date_of_birth,
				t1.passport_num,
				t1.passport_valid_to,
				t1.phone
			from clients_00 t1
			inner join v_clients t2
			on t1.client_id=t2.client_id
			and (
				t1.last_name<>t2.last_name
				or t1.first_name<>t2.first_name
				or t1.patronymic<>t2.patronymic
				or t1.date_of_birth<>t2.date_of_birth
				or t1.passport_num<>t2.passport_num
				or t1.passport_valid_to<>t2.passport_valid_to
				or t1.phone<>t2.phone)
	''')

def createTableClientsDeleted():
	cursor.execute('''
		create table clientsDeleted as
			select
				t1.client_id,
				t1.last_name,
				t1.first_name,
				t1.patronymic,
				t1.date_of_birth,
				t1.passport_num,
				t1.passport_valid_to,
				t1.phone
			from v_clients t1
			left join clients_00 t2 on t1.client_id=t2.client_id
			where t2.client_id is null
	''')

def createTableAccounts00():
	cursor.execute('''
		create table accounts_00 as
			select distinct
				account as account_num,
				account_valid_to as valid_to,
				client
			from table_trans
	''')

def createTableAccountsNew():
	cursor.execute('''
		create table accountsNew as
			select 
				t1.account_num,
				t1.valid_to,
				t1.client
			from accounts_00 t1
			left join v_accounts t2 on t1.client=t2.client
			where t2.client is null
	''')

def createTableAccountsUpdate():
	cursor.execute('''
		create table accountsUpdate as
			select distinct
				t1.account_num,
				t1.valid_to,
				t1.client
			from accounts_00 t1
			inner join v_accounts t2 on t1.client=t2.client
			and (t1.account_num<>t2.account_num
				or t1.valid_to<>t2.valid_to)
	''')

def createTableAccountsDeleted():
	cursor.execute('''
		create table accountsDeleted as
			select
				t1.account_num,
				t1.valid_to,
				t1.client
			from  v_accounts t1
			left join accounts_00 t2 on t1.client=t2.client
			where t2.client is null
	''')


def createTableCards00():
	cursor.execute('''
		create table cards_00 as
			select
				card as card_num,
				account as account_num
			from table_trans
	''')

def createTableCardsNew():
	cursor.execute('''
		create table cardsNew as
			select
				t1.card_num,
				t1.account_num
			from cards_00 t1
			left join v_cards t2 on t1.account_num=t2.account_num
			where t2.account_num is null
	''')

def createTableCardsUpdate():
	cursor.execute('''
		create table cardsUpdate as
			select 
				t1.card_num,
				t1.account_num
			from cards_00 t1
			inner join v_cards t2 on t1.account_num=t2.account_num
			and t1.account_num<>t2.account_num
	''')

def createTableCardsDeleted():
	cursor.execute('''
		create table cardsDeleted as
			select
				t1.card_num,
				t1.account_num
			from v_cards t1
			left join cards_00 t2 on t1.account_num=t2.account_num
			where t2.account_num is null
	''')
#_____________________________________________ 
# Удаление временных таблиц
#_____________________________________________

def clearDB():
	cursor.execute('drop table if exists clients_00')
	cursor.execute('drop table if exists clientsNew')
	cursor.execute('drop table if exists clientsUpdate')
	cursor.execute('drop table if exists clientsDeleted')

	cursor.execute('drop table if exists accounts_00')
	cursor.execute('drop table if exists accountsNew')
	cursor.execute('drop table if exists accountsUpdate')
	cursor.execute('drop table if exists accountsDeleted')

	cursor.execute('drop table if exists cards_00')
	cursor.execute('drop table if exists cardsNew')
	cursor.execute('drop table if exists cardsUpdate')
	cursor.execute('drop table if exists cardsDeleted')

#_____________________________________________ 
# Создание таблицы Report
#_____________________________________________
def createTableReport():
	cursor.execute('''
		CREATE table if not exists report(
				event_dt datetime, 
				passport varchar(64),
				fio varchar(64),
				phone varchar(64),
				event_type varchar(64),
				report_dt datetime
			)
	''')
#_____________________________________________ 
# Создание временной таблицы Fraud,
# которая составляет новый отчет
#_____________________________________________

def createTableFraud():
	cursor.execute('''
		create table fraud as
			select * from (
			select distinct
				date as event_dt,
				passport as passport_num,
				last_name||' '||first_name||' '||patronymic as fio,
				phone,
				case
					when passport_valid_to<date
						then 'passport_block'
					when account_valid_to<date
						then 'account_block'
					when
						city <> lag(city)over(partition by card order by trans_id)
						and strftime('%s',date)/60-strftime('%s',lag(date)over(partition by card order by trans_id))/60<60
						then 'location_fraud'
					when 
						oper_result='Успешно'
						and lag(oper_result,1)over(partition by card order by trans_id)='Отказ'
						and lag(oper_result,2)over(partition by card order by trans_id)='Отказ'
						and amount<lag(amount,1)over(partition by card order by trans_id)
						and lag(amount,1)over(partition by card order by trans_id)<lag(amount,2)over(partition by card order by trans_id)						
						and strftime('%s',date)/60-strftime('%s',lag(date)over(partition by card order by trans_id))/60<20
						then 'selection_fraud'
					end as event_type,
					date(current_timestamp) as report_dt
			from table_trans
			where passport not in (select passport_num from passport_blacklist)) t1
			where event_type is not null 
	''')	