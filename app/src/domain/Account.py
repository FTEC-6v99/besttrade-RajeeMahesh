class Account():
    def __init__(self, investor_id, balance, account_number = None):
        self.account_number = account_number
        self.investor_id = investor_id
        self.balance = balance 