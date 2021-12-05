from flask import Flask
from app.src.api.blueprints.investorbp import investor_bp
from app.src.api.blueprints.accountbp import account_bp
from app.src.api.blueprints.portfoliobp import portfolio_bp
from app.src.api.blueprints.stockbp import stock_bp
from app.src.api.blueprints.stockpricebp import stockprice_bp

app = Flask(__name__)

@app.route('/')
def health_check():
    return 'working'

app.register_blueprint(investor_bp)
app.register_blueprint(account_bp)
app.register_blueprint(portfolio_bp)
app.register_blueprint(stock_bp)
app.register_blueprint(stockprice_bp)


if __name__ == '__main__':
    app.run(port = 8080, debug=True)