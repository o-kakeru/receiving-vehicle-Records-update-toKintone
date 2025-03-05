'''
Created on 2024/07/19

@author: ookuma
'''
#import sys
#from PyQt5 import QtWidgets

import mysql.connector
import urllib.request
import json
from decimal import Decimal


class main:    
    
    DOMAIN = "crs-saitama"
    uri = "https://" + DOMAIN + ".cybozu.com/k/v1/records.json"
    api =  "aWcFwwqUMFs2uTRdeIuBwOOJdMmAJNjtpJG5Cjit"
    
    headers = {
        "Host": DOMAIN + ".cybozu.com:443",
        "X-Cybozu-API-Token": api,
        "Content-Type": "application/json",
    }
        
    # Decimal型のデータを文字列に変換
    @staticmethod
    def convert_decimal(obj):
        if isinstance(obj, Decimal):
            return str(obj)
            raise TypeError
    
    
    def getKintoneVehicleData(self,control_no):
        
        app = 77
        query = f"control_no = {control_no}"
    
        body = {
            "app": app,
            "query": query,
            "totalCount": True,
        }
    
        req = urllib.request.Request(
            url=main.uri,
            data=json.dumps(body).encode(),
            headers=main.headers,
            method="GET",
        )
    
        response = urllib.request.urlopen(req)
        res_dict = json.load(response)
        
        return res_dict
    
    
    def send_batch(self, records, method):
        # メソッドに応じてPUTまたはPOSTを実行
        body = {
            "app": 77,
            "records": records
        }
        
        req = urllib.request.Request(
            url=self.uri,
            data=json.dumps(body, default=main.convert_decimal).encode(),
            headers=self.headers,
            method=method
        )
        
        response = urllib.request.urlopen(req)
        json.load(response)
        #res_dict = json.load(response)
        #print(res_dict)
    
    
    def getCatsVehicleData(self):
        
        # DBへ接続
        cnx = mysql.connector.connect(
            user='cat_user',
            password='EyYNz6z9TpnMEvaU',
            host='CRSJPNCATBKSVR',
            database='cats_vehicle_management'
        )
        cursor = cnx.cursor(dictionary=True)
        
        sql = ('''
            SELECT  control_no , supplier_customer_code , supplier_customer_name_jp , takeover_customer_name_jp , manufacturer , car_model , type , chassis_number , model_year , 
                    vehicle_charts_remarks , driver_remarks , purchase_user_name_jp , acceptance_process , acceptance_pool , processing_division , purchase_price , deposit_price , 
                    settlement_division , tax_category , commission , vehicle_tax_epuivalent , weigth_tax_epuivalent , cali_epuivalent , takeover_date , warehousing_date ,
                    documents_receipt_date , documents_division , change_erasure_date , documents_expiration_date
            FROM    vehicle
            WHERE updated_at >= DATE_SUB(CURDATE(), INTERVAL 4 HOUR)
            ''')
            #WHERE warehousing_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR);
        cursor.execute(sql)
        dbRecords = cursor.fetchall()                                
        print(str(len(dbRecords)) + "件のデータを確認中...")    
        
        put_records = []
        post_records = []
        list_control_no = []
        
        for dbRec in dbRecords: 
            #処理する管理番号をリストに格納
            list_control_no.append(dbRec['control_no'])     
            
            cnx_cats_user = mysql.connector.connect(
                user='cat_user',
                password='EyYNz6z9TpnMEvaU',
                host='CRSJPNCATBKSVR',
                database='cats_master'
            )
            cats_user_cursor = cnx_cats_user.cursor(dictionary=True)
            
            cats_user_sql =('''
                SELECT login_id
                FROM cats_user
                WHERE abb_name_jp = %s
            ''')
            abb_name_jp = dbRec['purchase_user_name_jp']            
            
            cats_user_cursor.execute(cats_user_sql, (abb_name_jp,))
            cats_user = cats_user_cursor.fetchall()
            
            cats_login_id = cats_user[0]['login_id']
            cats_id = []
            cats_id.append(cats_login_id)            
                                                
            #DBから取得した管理番号をキーにkintone内のレコードを検索
            kntRec =self.getKintoneVehicleData(dbRec['control_no'])        
                                     
            #knt_control_no = kntRec['records'][0]['control_no']['value']
            
            #kintone上に管理番号(レコード)がある場合            
            if kntRec['records']:                            
                
                #文字列型の日付を日付型に変換
                if dbRec['takeover_date'] is None:
                    takeover_date_str = ""                            
                    takeover_date = ""
                else:
                    takeover_date_str = dbRec['takeover_date']                            
                    takeover_date = takeover_date_str.strftime("%Y-%m-%d")           
                
                if dbRec['warehousing_date'] is None:
                    warehousing_date_str =""
                    warehousing_date =  ""
                else:    
                    warehousing_date_str =dbRec['warehousing_date']                
                    warehousing_date =  warehousing_date_str.strftime("%Y-%m-%d")
                
                if dbRec['documents_receipt_date'] is None:  #書類受取日
                    documents_receipt_date_str = ""
                    documents_receipt_date = ""
                else:
                    documents_receipt_date_str = dbRec['documents_receipt_date']
                    documents_receipt_date = documents_receipt_date_str.strftime("%Y-%m-%d")
                
                if dbRec['change_erasure_date'] is None:  #名変・一抹日
                    change_erasure_date_str = ""
                    change_erasure_date = ""
                else:
                    change_erasure_date_str = dbRec['change_erasure_date']
                    change_erasure_date = change_erasure_date_str.strftime("%Y-%m-%d")

                if dbRec['documents_expiration_date'] is None:    #書類利用期限
                    documents_expiration_date_str = ""
                    documents_expiration_date = ""
                else:
                    documents_expiration_date_str = dbRec['documents_expiration_date']
                    documents_expiration_date = documents_expiration_date_str.strftime("%Y-%m-%d")

                                                                                                                                                        
                temp_put_body = {
                    "updateKey":{
                        "field": "control_no",
                        "value": dbRec['control_no']
                    },
                    "record": {                    
                        "manufacturer": {      #メーカー
                            "value": dbRec['manufacturer']
                        },
                        "car_model": {      #車種
                            "value": dbRec['car_model']
                        },
                        "type": {      #型式
                            "value": dbRec['type']
                        },
                        "chassis_number": {      #車台番号
                            "value": dbRec['chassis_number']
                        },
                        "model_year": {      #年式
                            "value": dbRec['model_year']
                        },
                        "purchase_user_name_jp": {      #担当営業
                            "value": dbRec['purchase_user_name_jp']
                        },
                        "customer_code": {  #仕入先コード
                            "value": dbRec['supplier_customer_code']
                        },
                        "supplier_customer_name_jp": {      #仕入先
                            "value": dbRec['supplier_customer_name_jp']
                        },
                        "takeover_customer_name_jp": {      #引取先
                            "value": dbRec['takeover_customer_name_jp']
                        },
                        "takeover_date": {      #引取予定日
                            "value": takeover_date
                        },
                        "warehousing_date": {      #入庫日
                            "value": warehousing_date
                        },
                        "acceptance_process": {      #受入工程
                            "value": dbRec['acceptance_process']
                        },
                        "acceptance_pool": {      #受入プール
                            "value":dbRec['acceptance_pool']
                        },
                        "processing_division": {      #処理区分
                            "value": dbRec['processing_division']
                        },                        
                        "purchase_price": {      #仕入金額
                            "value": dbRec['purchase_price']
                        },                        
                        "tax_category": {      #税区分
                            "value": dbRec['tax_category']
                        },
                        "deposit_price": {      #預託済金額
                            "value": dbRec['deposit_price']
                        },
                        "settlement_division": {      #決済区分
                            "value": dbRec['settlement_division']
                        },                        
                        "commission": {      #各種手数料
                            "value": dbRec['commission']
                        },                        
                        "vehicle_tax_epuivalent": {      #自動車税相当額
                            "value": dbRec['vehicle_tax_epuivalent']
                        },
                        "weigth_tax_epuivalent": {      #重量税相当額
                            "value": dbRec['weigth_tax_epuivalent']
                        },
                        "cali_epuivalent": {      #自賠責相当額
                            "value": dbRec['cali_epuivalent']
                        },
                        "vehicle_charts_remarks": {      #カルテ用備考
                            "value": dbRec['vehicle_charts_remarks']
                        },
                        "driver_remarks": {      #ドライバー注意事項
                            "value": dbRec['driver_remarks']
                        },
                        "kintone_id": {      #営業担当ユーザーID
                            "value": [
                                {"code":cats_login_id}
                                ]
                        },
                        "documents_receipt_date": {    # 書類受取日
                            "value": documents_receipt_date
                        },
                        "documents_division": {    # 書類区分
                            "value": dbRec['documents_division']
                        },
                        "change_erasure_date": {    # 名変・一抹日
                            "value": change_erasure_date
                        },
                        "documents_expiration_date": {    # 書類利用期限
                            "value": documents_expiration_date
                        }
                     }
                }
                put_records.append(temp_put_body)
                
                
            #管理番号が無い場合追加
            else:                   
                if dbRec['takeover_date'] is None:
                    takeover_date_str = ""                            
                    takeover_date = ""
                else:
                    takeover_date_str = dbRec['takeover_date']                            
                    takeover_date = takeover_date_str.strftime("%Y-%m-%d")           
                
                if dbRec['warehousing_date'] is None:
                    warehousing_date_str =""
                    warehousing_date =  ""
                else:    
                    warehousing_date_str =dbRec['warehousing_date']                
                    warehousing_date =  warehousing_date_str.strftime("%Y-%m-%d")    


                if dbRec['documents_receipt_date'] is None:  #書類受取日
                    documents_receipt_date_str = ""
                    documents_receipt_date = ""
                else:
                    documents_receipt_date_str = dbRec['documents_receipt_date']
                    documents_receipt_date = documents_receipt_date_str.strftime("%Y-%m-%d")
                
                if dbRec['change_erasure_date'] is None:  #名変・一抹日
                    change_erasure_date_str = ""
                    change_erasure_date = ""
                else:
                    change_erasure_date_str = dbRec['change_erasure_date']
                    change_erasure_date = change_erasure_date_str.strftime("%Y-%m-%d")

                if dbRec['documents_expiration_date'] is None:    #書類利用期限
                    documents_expiration_date_str = ""
                    documents_expiration_date = ""
                else:
                    documents_expiration_date_str = dbRec['documents_expiration_date']
                    documents_expiration_date = documents_expiration_date_str.strftime("%Y-%m-%d")

                
                temp_post_body = {
                    "control_no": {
                        "value": dbRec['control_no']
                    },
                    "manufacturer": {      #メーカー
                        "value": dbRec['manufacturer']
                    },
                    "car_model": {      #車種
                        "value": dbRec['car_model']
                    },
                    "type": {      #型式
                        "value": dbRec['type']
                    },
                    "chassis_number": {      #車台番号
                        "value": dbRec['chassis_number']
                    },
                    "model_year": {      #年式
                        "value": dbRec['model_year']
                    },
                    "purchase_user_name_jp": {      #担当営業
                        "value": dbRec['purchase_user_name_jp']
                    },
                    "customer_code": {  #仕入先コード
                        "value": dbRec['supplier_customer_code']
                    },
                    "supplier_customer_name_jp": {      #仕入先
                        "value": dbRec['supplier_customer_name_jp']
                    },
                    "takeover_customer_name_jp": {      #引取先
                        "value": dbRec['takeover_customer_name_jp']
                    },
                    "takeover_date": {      #引取予定日
                        "value": takeover_date
                    },
                    "warehousing_date": {      #入庫日
                        "value": warehousing_date
                    },
                    "acceptance_process": {      #受入工程
                        "value": dbRec['acceptance_process']
                    },
                    "acceptance_pool": {      #受入プール
                        "value":dbRec['acceptance_pool']
                    },
                    "processing_division": {      #処理区分
                        "value": dbRec['processing_division']
                    },                    
                    "purchase_price": {      #仕入金額
                        "value": dbRec['purchase_price']
                    },                    
                    "tax_category": {      #税区分
                        "value": dbRec['tax_category']
                    },
                    "deposit_price": {      #預託済金額
                        "value": dbRec['deposit_price']
                    },
                    "settlement_division": {      #決済区分
                        "value": dbRec['settlement_division']
                    },                    
                    "commission": {      #各種手数料
                        "value": dbRec['commission']
                    },                    
                    "vehicle_tax_epuivalent": {      #自動車税相当額
                        "value": dbRec['vehicle_tax_epuivalent']
                    },
                    "weigth_tax_epuivalent": {      #重量税相当額
                        "value": dbRec['weigth_tax_epuivalent']
                    },
                    "cali_epuivalent": {      #自賠責相当額
                        "value": dbRec['cali_epuivalent']
                    },
                    "vehicle_charts_remarks": {      #カルテ用備考
                        "value": dbRec['vehicle_charts_remarks']
                    },
                    "driver_remarks": {      #ドライバー注意事項
                        "value": dbRec['driver_remarks']
                    },
                    "kintone_id": {      #担当営業ユーザーid
                            "value": [
                                {"code":cats_login_id}
                            ]
                    },
                    "documents_receipt_date": {      #書類受取日
                        "value": documents_receipt_date
                    },
                    "driver_remarks": {      #書類区分
                        "value": dbRec['driver_remarks']
                    },
                    "driver_remarks": {      #名変・一抹日
                        "value": change_erasure_date
                    },
                    "driver_remarks": {      #書類利用期限
                        "value": documents_expiration_date
                    },                    
                }                              
                post_records.append(temp_post_body)        
                
        put_body = {
            "app" : 77,
            "records" : put_records
        } 
        
        post_body = {
            "app" : 77,
            "records" : post_records
        }
        
        #適応する管理番号    
        #print(list_control_no)
        
        # 100件ずつ処理
        batch_size = 100                          
        
        #put_bodyの中身が空でない場合実行
        if put_body['records']:
            
            for i in range(0, len(put_records), batch_size):
                print("データ適応中...")
                self.send_batch(put_records[i:i+batch_size], "PUT")                        
        
        #post_bodyの中身が空でない場合実行    
        if post_body['records']:    
            
            for i in range(0, len(post_records), batch_size):
                print("データ適応中...")
                self.send_batch(post_records[i:i+batch_size], "POST")
                            
        cursor.close()
        cnx.close()        
m = main()
m.getCatsVehicleData()

print("完了しました。")

                                         