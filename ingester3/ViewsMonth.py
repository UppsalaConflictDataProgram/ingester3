from datetime import datetime, date, timedelta

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

    @staticmethod
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

    @property
    def start_date(self):
        """
        Returns the ISO (YYYY-MM-DD) representation of the start date of a ViEWS Month
        :return: Start date in ISO format
        """
        return datetime.strftime(date(self.year, self.month, 1), '%Y-%m-%d')

    @property
    def end_date(self):
        """
        Returns the ISO (YYYY-MM-DD) representation of the end date of a ViEWS Month
        :return: End date in ISO format
        """
        #Start from 
        end_date = (date(self.year, self.month, 28)+timedelta(5)).replace(day=1)-timedelta(1)
        return datetime.strftime(end_date, '%Y-%m-%d')
    
    
