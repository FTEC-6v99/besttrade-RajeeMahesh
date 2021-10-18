# Database Access Object: file to interface with the database
# CRUD operations:
# C: Create
# R: Read
# U: Update
# D: Delete
import typing as t
from mysql.connector import connect, cursor
from mysql.connector.connection import MySQLConnection
import config
from app.src.domain.Investor import Investor
from app.src.domain.Account import Account
from app.src.domain.Portfolio import Portfolio

def get_cnx() -> MySQLConnection:
    return connect(**config.dbparams)

'''
    Investor DAO functions
'''

def get_all_investor() -> list[Investor]:
    '''
        Get list of all investors [R]
    '''
    investors: list[Investor] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from investor'
    cursor.execute(sql)
    results: list[dict] = cursor.fetchall()
    for row in results:
        investors.append(Investor(row['name'], row['status'], row['id']))
    db_cnx.close()
    return investors

def get_investor_by_id(id: int) -> t.Optional[Investor]:
    '''
        Returns an investor object given an investor ID [R]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from investor where id = %s'
    cursor.execute(sql, (id,))
    if cursor.rowcount == 0:
        return None
    else:
        row = cursor.fetchone()
        investor = Investor(row['name'], row['status'], row['id'])
        return investor 

def get_investors_by_name(name: str) -> list[Investor]:
    '''
        Return a list of investors for a given name [R]
    '''
    investors: list[Investor] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from investor where name = %s'
    cursor.execute(sql, (name,))
    if cursor.rowcount == 0:
        investors = []
    else:
        rows = cursor.fetchall()
        for row in rows:
            investors.append(Investor(row['name'], row['status'], row['id']))
    db_cnx.close()
    return investors


def create_investor(investor: Investor) -> None:
    '''
        Create a new investor in the db given an investor object [C]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'insert into investor (name, status) values (%s, %s)'
    cursor.execute(sql, (investor.name, investor.status))
    db_cnx.commit()
    db_cnx.close()

def delete_investor(id: int):
    '''
        Delete an investor given an id [D]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'delete from investor where id = %s'
    cursor.execute(sql, (id,))
    db_cnx.commit() # inserts, updates, and deletes
    db_cnx.close()

def update_investor_name(id: int, name: str) -> None:
    '''
        Updates the investor name [U]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update investor set name = %s where id = %s'
    cursor.execute(sql, (id, name))
    db_cnx.commit()
    db_cnx.close()

def update_investor_status(id: int, status: str) -> None:
    '''
        Update the inestor status [U]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update investor set status = %s where id = %s'
    cursor.execute(sql, (id, status))
    db_cnx.commit()
    db_cnx.close()

'''
    Account DAO functions
'''
def get_all_accounts() -> list[Account]:
    # Code goes here
    pass

def get_account_by_id(id: int) -> Account:
    # Code goes here
    pass

def get_accounts_by_investor_id(id: int) -> list[Account]:
    # Code goes here
    pass

def delete_account(id: int) -> None:
    # Code goes here
    pass

def update_acct_balance(id: int, bal: float) -> None:
    # Code goes here
    pass

def create_account(account: Account) -> None:
    # Code goes here
    pass

'''
    Portfolio DAO functions
'''
def get_all_portfolios() -> list[Portfolio]:
    # code goes here
    pass

def get_porfolios_by_acct_id(acct_id: int) -> list[Portfolio]:
    # code goes here
    pass

def get_portfolios_by_investor_id(investor_id: int) -> list[Portfolio]:
    # code goes here
    pass

def delete_portfolio(id: int) -> None:
    # code goes here
    pass

def buy_stock(ticker: str, price: float, quantity: int) -> None:
    # code goes here
    pass

def sell_stock(ticket: str, quantity: int, sale_price: float) -> None:
    # 1. update quantity in portfolio table
    # 2. update the account balance:
    # Example: 10 APPL shares at $1/share with account balance $100
    # event: sale of 2 shares for $2/share
    # output: 8 APPLE shares at $1/share with account balance = 100 + 2 * (12 - 10) = $104
    pass
