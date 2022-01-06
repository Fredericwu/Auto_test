import sqlite3
import numpy as np
import matplotlib as mpl
from prettytable.prettytable import from_db_cursor
mpl.use ('TKAgg')
import matplotlib.pyplot as plt
import yaml
import sql_chart_fred as gcdb
import utils
import log


class sql_graph_histogram (object):
    def __init__(self,Table_Names):
        # a_yaml_file = open('sql_config.yml')
        # a = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
        # self.table_n = a['Table_Names']
        self.table_n = Table_Names
        get_info = gcdb.Get_info_db(self.table_n)
        self.rwType_info = get_info.get_rwType_db()
        self.nj_info = get_info.get_nj_db()
        self.IOPS_4K_list = ['IOPS','4k']
        self.MBPS_1M_list = ['MBPS','1M']
        # fn_l,val=self.get_data_histogram(self.IOPS_4K_list)
        # print('fn_lllllll',fn_l)
        # print('valllllll',val)
        # self.draw_graph_histogram(fn_l,val)


    def get_data_histogram(self,IO_MB_list):
        con = sqlite3.connect ('sqldatabase_test.db')
        cur = con.cursor()
        self.IO_MB_list = IO_MB_list
        file_name_list = []
        self.drbd = []
        values = []
        
        for i in range(len(self.rwType_info)):
            nj_info ='"{}"'.format(self.nj_info[0])
            bs = '"{}"'.format(self.IO_MB_list[1])
            rw = '"{}"'.format(self.rwType_info[i])
            sql_sentence = 'SELECT DRBD_Type,' + ' ' + self.IO_MB_list[0] + ' ' + 'FROM Index_table,' +  self.table_n \
                + ' ' + 'WHERE Readwrite_type = ' + rw\
                + ' ' + 'AND Number_of_Job = ' + nj_info \
                + ' ' + 'AND IOdepth = "8"'\
                + ' ' + 'AND blocksize =' + bs \
                + ' ' + 'AND Index_table.Key_ID =' + ' ' + self.table_n + '.Key_ID' \

            # print (sql_sentence)
            sql_result = cur.execute(sql_sentence)
            file_name =  'histogram' + '-' + self.IO_MB_list[0] + '-' + self.rwType_info[i] + '-' + self.IO_MB_list[1] 
            file_name_list.append(file_name)
            for row in sql_result:
                self.drbd.append(row[0])
                values.append(row[1])
        # print('fffff1',file_name_list)
        # print ('vvvvvvvvv1',values)
        cur.close()
        con.commit()
        con.close()
        return file_name_list,values



    def draw_graph_histogram(self,fn_list,values):
        drbd_t = list(set(self.drbd))
        drbd_t.sort(key=self.drbd.index)
        num_of_device = len(drbd_t)
        plt.figure(figsize=(20,20), dpi = 100)
        bar_width = 0.3 

        flag = 0
        for b in [values[i:i + num_of_device] for i in range(0, len(values), num_of_device)]:
            # print('bbbbb',b)
            for i in range(len(drbd_t)):
                x_data = self.drbd[i]
                y_data = b[i]
                plt.bar(x_data, y_data, label=drbd_t[i],width = bar_width)
                plt.title(fn_list[flag])
                # print('xxxx',x_data)
                # print('yyyy',y_data)
                # print('tttttitle',fn_list[flag])
            plt.xlabel ('DRBD Type')
            # print(str(fn_list[0].split('-')))
            if 'IOPS' in str(fn_list[0].split('-')):
                plt.ylabel (self.IOPS_4K_list[0])
            else:
                plt.ylabel (self.MBPS_1M_list[0])

            plt.xticks (rotation = 30)
            plt.legend()
            plt.grid()
            plt.savefig(f"./performance_data/{fn_list[flag]}")
            str1 = 'Save ' +  fn_list[flag] + '.png to performance_data directory , Done'
            utils.write_log('', str1, 0)
            flag +=1
            if flag == len(self.rwType_info):
                flag = 0 
            plt.draw()
            plt.close(1)
            # plt.show()


def sql_graph_output():

    con = sqlite3.connect ('sqldatabase_test.db') # create connection object and database file
    cur = con.cursor() # create a cursor for connection object
    
    a_yaml_file = open('sql_config.yml')
    a = yaml.load(a_yaml_file, Loader = yaml.FullLoader)

    text_table = []
    drbd = []
    values = []

    for i in range(len(a['Table_hist'])):
        sql_sentence = 'SELECT Text_Table_Name, DRBD_Type,' + ' ' + a['Select_Data_2hist'] + ' ' + 'FROM Index_table,' +  a['Table_hist'][i] \
                + ' ' + 'WHERE Readwrite_type = ' + a['Readwrite_2hist']\
                + ' ' + 'AND Number_of_Job = "8"'\
                + ' ' + 'AND IOdepth = "8"'\
                + ' ' + 'AND blocksize =' + a['Blocksize_2hist'] \
                + ' ' + 'AND Index_table.Key_ID =' + ' ' + a['Table_hist'][i] + '.Key_ID' \
                    
        print (sql_sentence)
    
    # sql_sentence = 'SELECT Text_Table_Name, DRBD_Type,' + ' ' + a['Select_Data_2hist'] + ' ' + 'FROM Index_table,' +  a['Table_Name_2hist_1'] \
    #             + ' ' + 'WHERE Readwrite_type = ' + a['Readwrite_2hist']\
    #             + ' ' + 'AND Number_of_Job = "8"'\
    #             + ' ' + 'AND IOdepth = "8"'\
    #             + ' ' + 'AND blocksize =' + a['Blocksize_2hist'] \
    #             + ' ' + 'AND Index_table.Key_ID =' + a['Table_Name_2hist_1'] + '.Key_ID' \
    #             + ' ' + 'UNION ALL' \
    #             + ' ' + 'SELECT Text_Table_Name, DRBD_Type,' + a['Select_Data_2hist'] + ' ' + 'FROM Index_table,' + a['Table_Name_2hist_2'] \
    #             + ' ' + 'WHERE Readwrite_type = ' + a['Readwrite_2hist'] \
    #             + ' ' + 'AND Number_of_Job = "8"' \
    #             + ' ' + 'AND IOdepth = "8"' \
    #             + ' ' + 'AND blocksize =' + a['Blocksize_2hist'] \
    #             + ' ' + 'AND Index_table.Key_ID =' + a['Table_Name_2hist_2'] + '.Key_ID'
                         
        sql_result = cur.execute(sql_sentence)

        for row in sql_result:
            text_table.append(row[0])
            drbd.append(row[1])
            values.append(row[2])
            # print(row)

    # print (text_table)    
    # print (values)
    # print (drbd)

    
    plt.figure(figsize=(20,20), dpi = 100)
    bar_width = 0.3

    for i in range(len(drbd)):
        x_data = drbd[i]
        print (x_data)
        y_data = values[i]
        print (y_data)
        plt.bar(x_data, y_data, label = text_table[i], width = bar_width)
        
    plt.xlabel ('DRBD Type')
    plt.ylabel (a['Select_Data_2hist'])
    plt.xticks (rotation = 30)
    plt.title(a['Select_Data_2hist'] + ' ' + 'under Different DRBD Type (Blockszie =' + a['Blocksize_2hist'] + ')')
    plt.legend()
    plt.grid()
        
    # plt.savefig(a['Table_Name_2hist_1']-a['Table_Name_2hist_1']-a['Select_Data_2hist'].png)
    # plt.savefig('{Table_Names_1}-{Table_Names_2}-{Select_Data}.png', dpi = 200)
    plt.show()

    cur.close()
    con.commit()
    con.close()

if __name__ == '__main__':
    # sql_graph_output()
    sql_graph_histogram()



