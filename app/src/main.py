import app.src.db.dao as dao 

def main():
    inv = dao.get_all_investor()
    for i in inv:
        print(i.id)



if __name__ == '__main__':
    main()
