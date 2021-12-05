import json
from decimal import Decimal
from flask import Blueprint, request
import app.src.db.dao as dao
from app.src.domain.Account import Account

account_bp = bp = Blueprint('account',__name__,url_prefix='/account')

def default_json(t):
    return f'{t}'

### Fetch all the investor accounts
@bp.route('/get-all-accounts')
def get_accounts():
    accounts = dao.get_all_accounts()
    return json.dumps([account.__dict__ for account in accounts], default=default_json)

### Fetch the all accounts related to the selected Investor
@bp.route('/get-account-by-investor/<investor_id>')
def get_account_by_investor(investor_id):
    try:
        accounts = dao.get_accounts_by_investor_id(investor_id)
        return json.dumps([account.__dict__ for account in accounts], default=default_json)
    except Exception as e:
        return f'Failed to retrieve accounts by investor id: {str(e)}'

### Fetch the account details of the given account number
@bp.route('/get-account-by-accno/<account_number>')
def get_account_by_accno(account_number):
    try:
        accounts = dao.get_account_by_accno(account_number)
        return json.dumps([account.__dict__ for account in accounts], default=default_json)
    except Exception as e:
        return f'Failed to retrieve accounts by investor id: {str(e)}'

### Add additional funds to the account
@bp.route('/update-balance/<balance>/<account_number>')
def update_balance(balance, account_number):
    try:
        accounts = dao.update_acct_balance(balance, account_number)
        return json.dumps([account.__dict__ for account in accounts], default=default_json)
    except Exception as e:
        return f'Failed to update the balance: {str(e)}'

### Create new account for the given investor id
@bp.route('/create/<investor_id>', methods = ['POST']) 
def create_account(investor_id):
    q_params = request.args
    balance = 0
    if 'balance' in q_params:
        balance = q_params.get('balance')
    account = Account(investor_id, balance)
    try:
        dao.create_account(account)
        return 'OK', 200 
    except Exception as e:
        return 500, f'An error accoured while creating: {str(e)}'

### Delete the mentioned account number, but make sure to delete the relevant portfolio first
@bp.route('/delete-account/<accno>')
def delete_account(accno):
    try:
        dao.delete_account(accno)
        return 'OK', 200
    except Exception as e:
        return 500, f'Try deleting the corresponding portfolio first: {str(e)}'

