class Investor():
    # class has same attributes as the investor db table
    def __init__(self, name, status, id = None):
        self.id = id
        self.name = name
        self.status = status