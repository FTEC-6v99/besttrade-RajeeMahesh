from flask import Blueprint, render_template
from app.src.domain.Investor import Investor
import app.src.db.dao as dao


uibp = Blueprint('ui',__name__, url_prefix='/ui')

@uibp.route('/', methods = ['GET'])
def main():
    return render_template('home.html')

@uibp.route('/about', methods = ['GET'])
def about():
    return render_template('about.html')

@uibp.route('/investors', methods = ['GET'])
def investors():
    investors = dao.get_all_investor()
    return render_template('investors.html', investors = investors)  

@uibp.route('/accounts', methods = ['GET'])
def accounts():
    accounts = dao.get_all_accounts()
    return render_template('accounts.html', accounts = accounts)



