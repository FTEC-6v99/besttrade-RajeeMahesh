import json
from decimal import Decimal
from flask import Blueprint, request
import app.src.db.dao as dao
from app.src.domain.Stock import Stock
from app.src.domain.Stockprice import Stockprice

stockprice_bp = bp = Blueprint('stockprice',__name__,url_prefix='/stockprice')

def default_json(t):
    return f'{t}'

### Fetch the list of stock price details of all the stocks
@bp.route('/get-all')
def get_portfolios():
    stocks = dao.get_all_stockprice()
    return json.dumps([stock.__dict__ for stock in stocks], default=default_json)

### Fetch the list of stock price details of the choosen company 
@bp.route('/get-pricedetails-by-companyname/<company_name>')
def get_price_details(company_name):
    try:
        pricelist = dao.get_stockprice_by_company_name(company_name)
        return json.dumps([price.__dict__ for price in pricelist], default=default_json)
    except Exception as e:
        return f'Failed to retrieve price detail of the company selected: {str(e)}'

