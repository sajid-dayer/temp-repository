import csv
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile
from dateutil.parser import parse
import pandas as pd
import requests
import redis
from dateutil.relativedelta import relativedelta

from config import bhav_copy_url, redis_host, redis_password, redis_port, redis_db, bse_holiday, \
    number_of_records_to_save_in_redis


class BhavCopyDataDownload(object):
    def __init__(self):
        # connecting redis server using password.
        self.redis_data = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db,
                                            password=redis_password, health_check_interval=30)

    def is_weekday(self, date_t):
        """
        This function used to validate given date should not weekend.
        :param date_t: datetime obj.
        :return:
        """
        wk_day = date_t.weekday()
        if wk_day < 5:
            return True
        return False

    def is_bse_working(self, date_t):
        """
        This function used to validate if there is no  nation holiday.
        :param date_t:
        :return:
        """
        for date_str in bse_holiday:
            bse_date = parse(date_str)
            if date_t.date() == bse_date.date():
                return False
        return self.is_weekday(date_t)

    def last_working_day_of_bse(self, current_date):
        """
        This function used to get last working day of bse.
        :param current_date:
        :return:
        """
        current_date = current_date - relativedelta(days=1)
        while not self.is_bse_working(current_date):
            current_date = current_date - relativedelta(days=1)
        return current_date

    def str_data_as_csv_to_df(self, data, split_with='\n', start_with=0):
        """
        This function used to convert zip csv data to dataframe.
        :param data:
        :param split_with:
        :param start_with:
        :return:
        """
        ml = data.split(split_with)
        ml = ml[start_with:]
        final_list = []
        header = []
        for i, value in enumerate(ml):
            value = value.replace("\r", "")
            if i != 0:
                row = value.split(',')
                if len(header) != len(row):
                    spamreader = csv.reader(value, delimiter=',', quotechar='\"')
                    m = ""
                    for i, row in enumerate(spamreader):
                        if "," in row[0]:
                            row[0] = row[0].replace(",", "_")
                        m += ', '.join(row)
                    final_list.append(m.split(','))
                else:
                    final_list.append(row)
            else:
                header = value.split(',')
        df = pd.DataFrame(final_list, columns=header)
        return df

    def online_csv_inside_zip_to_df(self, url):
        """
        This function used to download zip file of bhav copy and fetch data from csv file.
        :param url:
        :return:
        """
        content = requests.get(url)
        f = ZipFile(BytesIO(content.content))
        data = ""
        for filename in f.namelist():
            data = (f.open(filename).read())
        df = self.str_data_as_csv_to_df(data.decode())
        return df

    def save_bhav_copy_data_to_redis(self, current_date):
        """
        This function used to save bhav copy data into redis(noSQL database)
        :return:
        """

        # We are not fetching current day data because it's update late in the evening.
        # For this reason for testing code only fetch last working data of bse.

        last_date_of_working = self.last_working_day_of_bse(current_date)

        url = bhav_copy_url.format(last_date_of_working.strftime('%d%m%y'))
        df = self.online_csv_inside_zip_to_df(url)
        df.rename(columns=lambda x: x.strip(), inplace=True)

        # convert dataframe to list of dict.
        list_of_dict = df.to_dict(orient='records')
        for i, stock_dict in enumerate(list_of_dict):
            try:
                self.redis_data.hmset(stock_dict['SC_NAME'].strip(), stock_dict)
                if number_of_records_to_save_in_redis != -1 and i == number_of_records_to_save_in_redis:
                    break
            except Exception as e:
                print e

    def main(self, current_date):
        self.save_bhav_copy_data_to_redis(current_date)


if __name__ == "__main__":
    current_date = datetime.utcnow() + relativedelta(minutes=330)
    obj = BhavCopyDataDownload()
    obj.main(current_date)
