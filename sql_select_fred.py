import sqlite3
import csv
import yaml
import Performance_get_config as gc
import sql_chart_fred as gcdb
from xlwt import Workbook
import xlrd
import xlwt





class sql_write_excel (object):
    def __init__(self,Table_Names,rw_type):
        self.table_n = Table_Names
        # a_yaml_file = open('sql_config.yml')
        # a = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
        # self.table_n = a['Table_Names']
        # self.rwType = 'write'
        # get_info = gcdb.Get_info_db(self.table_n)
        # self.excel_name = self.table_n + '_' + self.rwType + '.csv'
        self.rwType_info = rw_type
        # self.rwType_info = get_info.get_rwType_db()
        # self.device_list = get_info.get_device_db()
        # print('dddddl',self.device_list)
        # self.write_to_excel_all()
        # self.get_data_IO_MB()
        # self.rwType = 'write'

        # self.write_to_excel_bs()



    def write_to_excel_all(self):
        con = sqlite3.connect ('sqldatabase_test.db')
        cur = con.cursor()
        table = 'Index_table,' + self.table_n
        Excel_filename = self.table_n + '_' + 'all_data_excel'
        # Excel_filename = input ('Please Enter the name of the Excel file will be created:')
        for i in range(len(self.rwType_info)):
            rw = '"{}"'.format(self.rwType_info[i])
            statement = 'Index_table.Key_ID = '+ self.table_n + '.Key_ID' + ' '+ 'AND Readwrite_type =' + rw #+ ' ' +'ORDER BY IOPS'
            SQL_Sentence = 'select'+' '+ '*' +' '+'from'+' '+ table +' '+'where'+' '+ statement
            print('Sssssssss',SQL_Sentence)
            sql_result = cur.execute((SQL_Sentence))
            cur.execute(SQL_Sentence)

            with open(f"{Excel_filename}.csv","a+") as csv_file:
                csv_writer = csv.writer(csv_file, delimiter="\t")
                csv_writer.writerow([i[0] for i in cur.description])
                # print('descccc',cur.description)
                csv_writer.writerows(cur)
                # print('ccccccccur',cur)

        cur.close()
        con.commit()
        con.close()


    def get_data_IO_MB(self,rwType_bs):
        self.rwType = rwType_bs
        con = sqlite3.connect ('sqldatabase_test.db')
        cur = con.cursor()
        self.IOPS_list = []
        self.MBPS_list = []
        rwType_info ='"{}"'.format(self.rwType)
        # IO_MB_list = ['IOPS','MBPS']
        # for i in range(len(IO_MB_list)):
        #     if IO_MB_list[i] == 'IOPS':
        #         SQL_Sentence = 'select'+' '+ 'IOPS'+' '+'FROM'+' '+self.table_n +' '+'WHERE'+' '+ 'Readwrite_type='+ ' '+'"read"'
        #         sql_result = cur.execute((SQL_Sentence))
        #         for row in sql_result:
        #             IOPS_list.append(row[0])
        #     else:
        #         SQL_Sentence = 'select'+' '+ 'MBPS'+' '+'FROM'+' '+self.table_n +' '+'WHERE'+' '+ 'Readwrite_type='+ ' '+'"read"'
        #         sql_result = cur.execute((SQL_Sentence))
        #         for row in sql_result:
        #             MBPS_list.append(row[0])

        SQL_Sentence = 'select'+' '+ 'IOPS'+' '+'FROM'+' '+self.table_n +' '+'WHERE'+' '+ 'Readwrite_type='+ ' '+rwType_info
        sql_result = cur.execute((SQL_Sentence))
        for row in sql_result:
            # print('rrrrrrow',row)
            self.IOPS_list.append(row[0])

        SQL_Sentence = 'select'+' '+ 'MBPS'+' '+'FROM'+' '+self.table_n +' '+'WHERE'+' '+ 'Readwrite_type='+ ' '+rwType_info
        sql_result = cur.execute((SQL_Sentence))
        for row in sql_result:
            # print('rrrrrrow',row)
            self.MBPS_list.append(row[0])


        # print('IOPssssss',IOPS_list)
        # print('MBPssssss',MBPS_list)

        cur.close()
        con.commit()
        con.close()


    def write_to_excel_bs(self,dict_data,rwType,iodepth,numjobs):
        # self.excel_name = '222222-read.csv'
        self.dict_data = dict_data
        self.rwType = rwType
        self.iodepth = iodepth
        self.numjobs = numjobs
        self.excel_name = self.table_n + '_' + self.rwType + '.csv'
        # Performance_config = gc.Self_defined_scenarios('./config/P_self_defined_scenario.yml') #need to known scenarios
        # self.dict_data = Performance_config.perforamce_setting() #bs form theis config
        # print(self.dict_data)


        self.parameter_dict =  {'filename': '\\w+', 'rw': self.rwType, 'bs': '\\w+', 'iodepth': self.iodepth, 'numjobs': self.numjobs}

        wb = Workbook(encoding='utf-8')
        table = wb.add_sheet('IOPS',cell_overwrite_ok=True)
        sheet = wb.add_sheet('MBPS',cell_overwrite_ok=True)
        attri=['bs','iodepth','numjobs']
        for value in attri:
            # print('vvvv',value)
            # print('ssssssssssspppddd',self.parameter_dict[value])
            if self.parameter_dict[value]=='\w+':
                for i in range(len(self.dict_data[value])):
                    # print('ddddvvv',self.MBPS_list)
                    sheet.write(1,i+1,self.dict_data[value][i]) 
                    sheet.write(2,i+1,self.MBPS_list[i])  
                    table.write(1,i+1,self.dict_data[value][i]) 
                    table.write(2,i+1,self.IOPS_list[i]) 
                    number=len(self.MBPS_list)/len(self.dict_data[value])
                    for row in range(int(number)):
                        sheet.write(2+row,i+1,self.MBPS_list[i+len(self.dict_data[value])*row])
                        table.write(2+row,i+1,self.IOPS_list[i+len(self.dict_data[value])*row]) 
        attri_l=['rw','filename']
        for value in attri_l:
            if self.parameter_dict[value]=='\w+':
                for i in range(len(self.dict_data[value])):
                    sheet.write(i+2,0,self.dict_data[value][i])
                    table.write(i+2,0,self.dict_data[value][i]) 
        # wb.save(self.excel_name)
        list_param=[]
        for k,v in self.parameter_dict.items():
            if self.parameter_dict[k]!='\w+':
                list_param.append(f'{k}={v}')
        sheet.write_merge(0,0,0,5,','.join(list_param),self.excel_style())
        table.write_merge(0,0,0,5,','.join(list_param),self.excel_style())
        wb.save(self.excel_name)
    
    def excel_style(self):
        style = xlwt.XFStyle() # 初始化样式
        al = xlwt.Alignment()
        al.horz = 0x02     
        al.vert = 0x01      
        style.alignment = al
        return style
 


def sql_analysis_output():

    con = sqlite3.connect ('sqldatabase_test.db') # create connection object and database file
    cur = con.cursor() # create a cursor for connection object

    # Select_Data = input ('Please Enter the selected data you want:')
    # Table_Names = input ('Please Enter the Table Name:')
    # Condition = input ('Please Enter the Condition for SQL Sentence:')
    # SQL_Sentence = rf'SELECT {Select_Data} FROM {Table_Names} WHERE {Condition}'
    # sql_result = cur.execute(SQL_Sentence)
    # sql_result = cur.execute('SELECT * FROM Index_Table,Guangzhou_20210924_RAM WHERE Index_Table.Key_ID = Guangzhou_20210924_RAM.Key_ID AND DRBD_Type = "drbd1001" AND Readwrite_type = "read" AND Number_of_Job = "8" AND IOdepth = "8"')

    a_yaml_file = open('sql_config.yml')
    a = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
    SQL_Sentence = 'select'+' '+a['wanted data']+' '+'from'+' '+a['table'] +' '+'where'+' '+a['statement']
    print('Sssssssss',SQL_Sentence)
    sql_result = cur.execute((SQL_Sentence))
    
    # columnlist = []
    # for column in sql_result.description:
    #     columnlist.append(column[0])
    # print ('ccccc',columnlist)

    # for row in sql_result:
    #     print ('rrrrrrr',row)

    Excel_filename = input ('Please Enter the name of the Excel file will be created:')
    
    cur.execute(SQL_Sentence)
    with open(f"{Excel_filename}.csv","w") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter="\t")
        csv_writer.writerow([i[0] for i in cur.description])
        # print('descccc',cur.description)
        csv_writer.writerows(cur)
        # print('ccccccccur',cur)

    cur.close()
    con.commit()
    con.close()

if __name__ == '__main__':
    # sql_print_index()
    # sql_analysis_output()
    sql_write_excel()




