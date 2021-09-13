from datetime import datetime

class ViewsMonth(object):

    def __init__(self, id):
        self.__validate(int(id))
        self.id = int(id)
        self.month = self.id2month(id)
        self.year = self.id2year(id)

    def __repr__(self):
        return f'ViewsMonth({self.id})'

    def __str__(self):
        return f'ViewsMonth(id={self.id}) #=> year:{self.year}, month:{self.month}'

    @classmethod
    def id2month(cls, id):
        return (id - 1) % 12 + 1

    @classmethod
    def id2year(cls, id):
        return int((id - 1) / 12) + 1980

    @staticmethod
    def __validate(id):
        if int(id) <= 0:
            raise ValueError("Monthid cannot be negative")

    @staticmethod
    def __validate_year(year):
        if year < 1980:
            raise ValueError("Year must be >=1980")

    def __validate_month(month):
        if not (1 <= month <= 12):
            raise ValueError("Month must be between 1 and 12")

    @classmethod
    def from_year_month(cls, year, month):
        """
        A factory returning a ViewsMonth object with the proper id.
        """
        cls.__validate_year(year)
        cls.__validate_month(month)
        return cls(int((year - 1980) * 12 + month))

    @classmethod
    def from_date(cls, datetime):
        return cls.from_year_month(datetime.year, datetime.month)

    @classmethod
    def now(cls):
        return cls.from_date(datetime.now())