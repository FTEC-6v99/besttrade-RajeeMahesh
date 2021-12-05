# create blueprint for investor resources 
import json
from flask import Blueprint, request
import app.src.db.dao as dao
from app.src.domain.Investor import Investor

investor_bp = bp = Blueprint('investor',__name__,url_prefix='/investor')

### Fetch all the investor details registered in this website
@bp.route('/get-all')
def get_investors():
    investors = dao.get_all_investor()
    return json.dumps([investor.__dict__ for investor in investors])

### Fetch the investor detail for the selected investor id
@bp.route('/get-by-id/<id>')
def get_investor(id):
    investor = dao.get_investor_by_id(id)
    return json.dumps([i.__dict__ for i in investor])

### Update Investor status
@bp.route('/update-investor/<id>/<status>', methods = ['POST'])
def update_investor_status(id, status):
    try:
        investors = dao.update_investor_status(id, status)
        return json.dumps([investor.__dict__ for investor in investors])
    except Exception as e:
        return f'Failed to update investor status: {str(e)}'

### Create new investor
@bp.route('/create/<name>', methods = ['POST']) 
def create_investor(name):
    q_params = request.args
    status = 'ACTIVE'
    if 'status' in q_params:
        status = q_params.get('status')
    investor = Investor(name, status)
    try:
        dao.create_investor(investor)
        return 'OK', 200 
    except Exception as e:
        return 500, f'An error accoured while creating: {str(e)}'



### Delete the investor detail, but make sure to delte the relevant accounts and portfolios of the investor first
@bp.route('/delete/<id>', methods = ['DELETE'])
def delete_investor(id):
    try:
        dao.delete_account(id)
        return 'OK', 200
    except Exception as e:
        return 500, f'Try deleting trading accounts of the investor first: {str(e)}'