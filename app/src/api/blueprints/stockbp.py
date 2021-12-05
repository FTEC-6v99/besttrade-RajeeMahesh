import json
from decimal import Decimal
from flask import Blueprint, request
import app.src.db.dao as dao
from app.src.domain.Stock import Stock

stock_bp = bp = Blueprint('stock',__name__,url_prefix='/stock')

def default_json(t):
    return f'{t}'

### Fetch the list of available stocks
@bp.route('/get-all')
def get_portfolios():
    stocks = dao.get_all_stocks()
    return json.dumps([stock.__dict__ for stock in stocks], default=default_json)