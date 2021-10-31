import app.src.db.dao as dao 

def main():
    inv = dao.get_all_investor()
    for i in inv:
        print(f'Id: {i.id} - NAME----name: {i.name} - status: {i.status}')

    
    acc = dao.get_all_accounts()
    for a in acc:
        print(f'account no: {a.account_number} - invester id: {a.investor_id} - balance: {a.balance} ')

    dao.buy_stock(7, 'T', 5, 9.1)
    dao.sell_stock(3, 'RECO', 5, 7.00)


if __name__ == '__main__':
    main()
