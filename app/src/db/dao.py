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
    sql: str = 'select * from investor_detail'
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
    investor = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from investor_detail where id = %s'
    cursor.execute(sql, (id,))
    rows = cursor.fetchall()
    if cursor.rowcount == 0:
        print('Not found')
    else:
        for row in rows:
            investor.append(Investor(row['name'], row['status'], row['id']))
    db_cnx.close()
    return investor 

def get_investors_by_name(name: str) -> list[Investor]:
    '''
        Return a list of investors for a given name [R]
    '''
    investors: list[Investor] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from investor_detail where name = %s'
    cursor.execute(sql, (name,))
    rows = cursor.fetchall()
    if cursor.rowcount == 0:
        print('No such name found')
        investors = []
    else:
        for row in rows:
            investors.append(Investor(row['name'], row['status'], row['id']))
    db_cnx.close()
    return investors 


def create_investor(investor: Investor) -> None:
    '''
        Create a new investor in the db given an investor object [C]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'insert into investor_detail (name, status) values (%s, %s)'
    cursor.execute(sql, (investor.name, investor.status))
    db_cnx.commit()
    db_cnx.close()

def delete_investor(id: int):
    '''
        Delete an investor given an id [D]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'delete from investor_detail where id = %s'
    cursor.execute(sql, (id,))
    db_cnx.commit() # inserts, updates, and deletes
    db_cnx.close()

def update_investor_name(id: int, name: str) -> None:
    '''
        Updates the investor name [U]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update investor_detail set name = %s where id = %s'
    cursor.execute(sql, (name, id,))
    db_cnx.commit()
    db_cnx.close()

def update_investor_status(id: int, status: str) -> None:
    '''
        Update the inestor status [U]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update investor_detail set status = %s where id = %s'
    cursor.execute(sql, (status, id))
    db_cnx.commit()
    db_cnx.close()

'''
    Account DAO functions
'''
def get_all_accounts() -> list[Account]:
    '''
        Get list of all Accounts [R]
    '''
    accounts: list[Account] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from account'
    cursor.execute(sql)
    results: list[dict] = cursor.fetchall()
    for row in results:
        accounts.append(Account(row['investor_id'], row['balance'], row['account_number']))
    db_cnx.close()
    return accounts
    
def get_account_by_id(account_number: int) -> Account:
    '''
    Get list of all Accounts by Account_Number [R]
    '''
    account: list[Account] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from account where account_number = %s'
    cursor.execute(sql, (account_number,))
    rows: list[dict]= cursor.fetchall()
    if cursor.rowcount == 0:
        print('No such account number found in the database!')
    else:
        for row in rows:
            account.append(Account(row['account_number'], row['investor_id'], row['balance']))
    db_cnx.close()
    return account 

def get_accounts_by_investor_id(investor_id: int) -> list[Account]:
    '''
    Get list of all Accounts by investor id [R]
    '''
    accounts: list[Account] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from account where investor_id = %s'
    cursor.execute(sql, (investor_id,))
    rows: list[dict] = cursor.fetchall()
    if cursor.rowcount == 0:
        print('No such account number found in the database!')
    else:
        for row in rows:
            accounts.append(Account(row['investor_id'], row['balance'], row['account_number']))
    db_cnx.close()
    return accounts 

def delete_account(id: int) -> None:
    '''
    Delete row in the account table by investor id [D]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'delete from account where id = %s'
    cursor.execute(sql, (id,))
    db_cnx.commit() # inserts, updates, and deletes
    db_cnx.close()

def update_acct_balance(balance: float, account_number: int) -> None:
    '''
    Updates the balance of the account given the account number [U]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update account set balance = %s where account_number = %s'
    cursor.execute(sql, (balance, account_number,))
    db_cnx.commit() # inserts, updates, and deletes
    db_cnx.close()

def create_account(account: Account) -> None:
    '''
    Creates the new account for new investor with the availble balance to trade [C]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'insert into account(investor_id, balance) values(%s, %s)'
    cursor.execute(sql, (account.investor_id, account.balance))
    db_cnx.commit() # inserts, updates, and deletes
    db_cnx.close()    

'''
    Portfolio DAO functions
'''
def get_all_portfolios() -> list[Portfolio]:
    '''
    Get list of all Portfolio [R]
    '''
    accounts: list[Portfolio] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from portfolio'
    cursor.execute(sql)
    results: list[dict] = cursor.fetchall()
    for row in results:
        accounts.append(Portfolio(row['account_number'], row['ticker'], row['quantity'], row['purchase_price']))
    db_cnx.close()
    return accounts

def get_porfolios_by_acct_id(acct_id: int) -> list[Portfolio]:
    '''
    Get list of all Portfolios by the account_number [R]
    '''
    accounts: list[Portfolio] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from portfolio where account_number = %s'
    cursor.execute(sql, (acct_id,))
    results: list[dict] = cursor.fetchall()
    if results.count == 0:
        print("No portfolio exist for the account number given")
    else:
        for row in results:
            accounts.append(Portfolio(row['account_number'], row['ticker'], row['quantity'], row['purchase_price']))
    db_cnx.close()
    return accounts

def get_portfolios_by_investor_id(investor_id: int) -> list[Portfolio]:
    '''
    Get list of all Portfolios by investor id by joining tables [R]
    '''
    accounts: list[Portfolio] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = '''select a.investor_id, p.account_number, ticker, quantity, purchase_price
                from portfolio p 
                join account a 
                on a.account_number = p.account_number
                where investor_id = %s'''
    cursor.execute(sql, (investor_id,))
    results: list[dict] = cursor.fetchall()
    if results.count == 0:
        print("No portfolio exist for the Investor id given")
    else:
        for row in results:
            accounts.append(Portfolio(row['account_number'], row['ticker'], row['quantity'], row['purchase_price']))
    db_cnx.close()
    return accounts

def delete_portfolio(id: int, ticker: str) -> None:
    '''
    Delete the Portfolios by account number and ticker [D]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'delete from portfolio where account_number = %s and ticker = %s'
    cursor.execute(sql, (id, ticker))
    db_cnx.commit() # inserts, updates, and deletes
    db_cnx.close()

def buy_stock(id: int, ticker_to_buy: str,  quantity: int, price: float) -> None:
    """
    When buy stock is clicked with mentioning
        1. investor_id
        2. ticker_to_buy
        3. quantity
        4. price
        there are changes made in 3 tables 
            1. portfolio table -> updated with additional stocks bought
            2. stock_price table -> updated with the decrease in stock volume 
            3. Investor account table -> updated with the reduced balance due to buying of stock
    [C R U]
    """

    def find_account_number(id: int) -> list[dict]: 
        '''
        Helps in fetching account number of the investor who logged in to buy stocks [R]
        '''
        db_cnx: MySQLConnection = get_cnx()
        cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
        sql: str = 'select account_number from account where investor_id = %s'
        cursor.execute(sql, (id,))
        account_number: list[dict] = cursor.fetchall()
        db_cnx.close()
        return account_number 

    def create_portfolio(id: int, ticker_to_sell: str, quantity: int) -> None: 
        '''
        Creates new row in the portfolio table [C]
        '''
        db_cnx: MySQLConnection = get_cnx()
        cursor = db_cnx.cursor()
        account_number: list[dict] = find_account_number()
        sql = 'insert into portfolio values(%s, %s, %s, %s)'
        cursor.execute(sql, (account_number[0]['account_number'], ticker_to_buy, quantity, price))
        db_cnx.commit() # inserts, updates, and deletes
        db_cnx.close()

    def update(quantity: int, ticker_to_buy: str, id: int) -> None: 
        '''
        updates a row which matches account_number and ticker_to_buy with the portfolio table [U]
        '''       
        db_cnx: MySQLConnection = get_cnx()
        cursor = db_cnx.cursor()
        account_number: list[dict] = find_account_number()
        sql = '''update portfolio set quantity = quantity + %s 
                where ticker = %s and account_number = %s'''
        cursor.execute(sql, (quantity, ticker_to_buy, account_number[0]['account_number']))
        db_cnx.commit() # inserts, updates, and deletes
        db_cnx.close()

    def update_portfolio():
        '''
        On clicking buy stock, either of these two actions take place in portfolio table
         -> Server checks whether the ticker is already present for this investor
                if yes -> update the no of quantity -> update [U]
                else -> it goes ahead to create new row -> create portfolio [C]
        '''
        db_cnx: MySQLConnection = get_cnx()
        cursor = db_cnx.cursor(dictionary = True)
        sql = '''select ticker, account_number from portfolio'''
        cursor.execute(sql)
        rows: list[dict] = cursor.fetchall()
        account_number: list[dict] = find_account_number()
        db_cnx.close()
        for row in range(len(rows)):
            if rows[row]['ticker'] == ticker_to_buy and rows[row]['account_number'] == account_number[0]['account_number']:
                update()
                break
        else:
            print("Creating New Portfolio !")
            create_portfolio()

    def cal_account_balance(quantity: int, price: float, id: int) -> None:
        '''
        Updates balance in the account table with 
        new balance = old balance - (quantity * price) [U]
        '''
        stock_buy_rate = quantity * price
        db_cnx: MySQLConnection = get_cnx()
        cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
        account_number: list[dict] = find_account_number()
        sql = 'update account set balance = balance - %s where account_number = %s'
        cursor.execute(sql, (stock_buy_rate, account_number[0]['account_number']))
        db_cnx.commit() # inserts, updates, and deletes
        db_cnx.close()

    def stock_volume(quantity: int, ticker_to_buy: str):
        '''
        Updates stock volume in stock_price table with 
        new stock = old volume - quantity [U]
        '''
        db_cnx: MySQLConnection = get_cnx()
        cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
        sql = '''update stock_price set volume = volume - %s 
                    where stock_id = (select stock_id from stock where ticker = %s)'''
        cursor.execute(sql, (quantity, ticker_to_buy))
        db_cnx.commit() # inserts, updates, and deletes
        db_cnx.close()

    
    cal_account_balance()                
    stock_volume()
    update_portfolio()

def sell_stock(id: int, ticker_to_sell: str, quantity: int, sale_price: float) -> None:
    # 1. update quantity in portfolio table
    # 2. update the account balance:
    # Example: 10 APPL shares at $1/share with account balance $100
    # event: sale of 2 shares for $2/share
    # output: 8 APPLE shares at $1/share with account balance = 100 + 2 * (12 - 10) = $104

    '''
    When sell stock is clicked with mentioning
        1. investor_id
        2. ticker_to_sell
        3. quantity
        4. price
        there are changes made in 3 tables 
            1. portfolio table -> updated with stocks sold -> reducinng quantity of ticker sold
            2. stock_price table -> updated with the increase in stock volume 
            3. Investor account table -> updated with the increased balance due to selling of stock
    [R U D]
    '''

    def find_account_number(id: int) -> list[dict]:
        '''
        Helps in fetching account number of the investor who logged in to buy stocks [R]
        '''
        db_cnx: MySQLConnection = get_cnx()
        cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
        sql: str = 'select account_number from account where investor_id = %s'
        cursor.execute(sql, (id,))
        account_number: list[dict] = cursor.fetchall()
        db_cnx.close()
        return account_number

    def delete_portfolio(id: int, ticker_to_sell: str) -> None:
        '''
        deletes portfolio by account number and ticker [D]
        '''
        db_cnx: MySQLConnection = get_cnx()
        cursor = db_cnx.cursor()
        account_number: list[dict] = find_account_number()
        sql = 'delete from portfolio where account_number = %s and ticker = %s'
        cursor.execute(sql, (account_number[0]['account_number'], ticker_to_sell))
        db_cnx.commit() # inserts, updates, and deletes
        db_cnx.close()

    def update_portfolio(id: int, ticker_to_sell: str) -> None:
        '''
        On clicking sell stock -> server checks the quantiy of the ticker for the particular 
        investor -> either of these two actions take place,
            if quantity = 0 -> delete_portfolio() [D]
            else updates the row with the reduced quantity [U]        
        '''

        db_cnx: MySQLConnection = get_cnx()
        cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
        account_number: list[dict] = find_account_number()
        sql: str = 'select quantity from portfolio where account_number = %s and ticker = %s'
        cursor.execute(sql, (account_number[0]['account_number'], ticker_to_sell))
        fetch_quantity: list[dict] = cursor.fetchall()
        db_cnx.close()
        if fetch_quantity[0]['quantity'] == 0:
            delete_portfolio()
        else: 
            db_cnx: MySQLConnection = get_cnx()
            cursor = db_cnx.cursor(dictionary=True)
            sql = 'update portfolio set quantity = quantity - %s where account_number = %s and ticker = %s'
            cursor.execute(sql, (quantity, account_number[0]['account_number'], ticker_to_sell))
            db_cnx.commit()
            db_cnx.close()

    def cal_account_balance(quantity: int, sale_price: float, id: int):
        '''
        Updates balance in the account table with 
        new balance = old balance + (quantity * price) [U]
        '''
        stock_sell_rate = quantity * sale_price
        db_cnx = get_cnx()
        cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
        account_number: dict[list] = find_account_number()
        sql: str = 'update account set balance = balance + %s where account_number = %s'
        cursor.execute(sql, (stock_sell_rate, account_number[0]['account_number']))
        db_cnx.commit() # inserts, updates, and deletes
        db_cnx.close()

    def stock_volume(quantity: int, ticker_to_sell: str):
        '''
        Updates stock volume in stock_price table with 
        new stock = old volume + quantity [U]
        '''
        db_cnx: MySQLConnection = get_cnx()
        cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
        sql = '''update stock_price set volume = volume + %s 
                    where stock_id = (select stock_id from stock where ticker = %s)'''
        cursor.execute(sql, (quantity, ticker_to_sell))
        db_cnx.commit() # inserts, updates, and deletes
        db_cnx.close()
    
    stock_volume()
    cal_account_balance()
    update_portfolio() 








