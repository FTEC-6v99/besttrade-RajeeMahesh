import json
from decimal import Decimal
from flask import Blueprint, request
import app.src.db.dao as dao
from app.src.domain.Portfolio import Portfolio

portfolio_bp = bp = Blueprint('portfolio',__name__,url_prefix='/portfolio')

def default_json(t):
    return f'{t}'

### Fetch all the portfolios created 
@bp.route('/get-all-portfolio')
def get_portfolios():
    portfolios = dao.get_all_portfolios()
    return json.dumps([portfolio.__dict__ for portfolio in portfolios], default=default_json)

### Get the portfolios of the given account number
@bp.route('/get-portfolio-by-accno/<acc_no>')
def get_portfolio_by_accno(acc_no):
    try:
        portfolios = dao.get_porfolios_by_acct_id(acc_no)
        return json.dumps([portfolio.__dict__ for portfolio in portfolios], default=default_json)
    except Exception as e:
        return f'Failed to retrieve portfolio by investor id: {str(e)}'

### Get all the portfolios of the given investor id
@bp.route('/get-portfolio-by-investorid/<investor_id>')
def get_portfolio_by_investorid(investor_id):
    try:
        portfolios = dao.get_portfolios_by_investor_id(investor_id)
        return json.dumps([portfolio.__dict__ for portfolio in portfolios], default=default_json)
    except Exception as e:
        return f'Failed to retrieve accounts by investor id: {str(e)}'

# delete portfolio related to the given account number
@bp.route('/delete-portfolio/<accno>/<ticker>')
def delete_portfolio(accno, ticker):
    try:
        dao.delete_portfolio(accno, ticker)
        return 'OK', 200
    except Exception as e:
        return 500, f'Failed to the delete the portfolio: {str(e)}'

