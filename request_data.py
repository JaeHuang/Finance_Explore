import requests
import pandas as pd
from google.cloud import bigquery

class GetData:
    def __init__(self):
        self.columns = [('Timestamp','出表日期'),
                        ('DataTime','資料年月'),
                        ('Code','公司代號'),
                        ('Name','公司名稱'),
                        ('Industry','產業別'),
                        ('Sales','營業收入-當月營收'),
                        ('MoM','營業收入-上月比較增減(%)'),
                        ('YoY','營業收入-去年同月增減(%)')]
    def get_data(self):
        path = 'https://openapi.twse.com.tw/v1/opendata/t187ap05_L'
        header = {'produces': 'application/json'}
        res = requests.get(path, params=header)

        data = res.json()
        data = self.format_data(data)
        return data

    def extract_dict(self, row:dict)->dict:
        output_dict = {}
        for eng_col, chn_col in self.columns:
            output_dict[eng_col] = row[chn_col]
        return output_dict

    def format_date(self, entry:dict)->dict:
        try:
            year = str(int(entry['Timestamp'][:-4]) + 1911)
            month = entry['Timestamp'][-4:-2]
            date = entry['Timestamp'][-2:]
            entry['Timestamp'] = f'{year}-{month}-{date}'
            entry['DataTime'] = str(entry['DataTime'])

            entry['Sales'] = int(entry['Sales'])
            entry['MoM'] = float(entry['MoM'])
            entry['YoY'] = float(entry['YoY'])
        except ValueError:
            print(entry)
        return entry


    def format_data(self, data_list:list)->list:
        data = list(map(lambda x: self.extract_dict(x), data_list))
        data = list(map(lambda x: self.format_date(x), data))
        return data

    def set_cloud_cred(self):
        self.bqclient = bigquery.Client('finance-explore')

    def set_table_schema(self):
        schema = [
            {"description":"Report generate date",
             "mode":"Required",
             "name":"Timestamp",
             "type":"Date"
             },
            {"description":"Report date",
             "mode":"Required",
             "name":"DataTime",
             "type":"String"
             },
            {"description":"Company code",
             "mode":"Required",
             "name":"Code",
             "type":"String"
             },
            {"description":"Company name",
             "mode":"Required",
             "name":"Name",
             "type":"String"
             },
            {"description":"Company industry",
             "mode":"Nullable",
             "name":"Industry",
             "type":"String"
             },
            {"description":"",
             "mode":"Required",
             "name":"Sales",
             "type":"Integer"
             },
            {"description":"",
             "mode":"Nullable",
             "name":"MoM",
             "type":"Float"
             },
            {"description":"",
             "mode":"Nullable",
             "name":"YoY",
             "type":"Float"
             }

        ]
        return schema

    def upload_data(self, data):
        schema = self.set_table_schema()
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema
        )

        job = self.bqclient.load_table_from_json(data,'Sales.Sales', job_config=job_config)
        print(job.result())




if __name__ == '__main__':
    getter = GetData()
    data = getter.get_data()
    getter.set_cloud_cred()
    getter.upload_data(data)
    # print(data[:10])
    # print(getter.bqclient)