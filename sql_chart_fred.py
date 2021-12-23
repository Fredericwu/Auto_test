import sqlite3
import numpy as np
import matplotlib
matplotlib.use ('TKAgg')
import matplotlib.pyplot as plt
import yaml
import index as ind


print_info = ind.sql_print_index()

class Get_info_db (object):
    def __init__(self,Table_Names):
        self.Table_Names = Table_Names
        self.get_bs_db()
        self.get_rwType_db()
        self.get_nj_db()
        self.get_IOdepth_db()


    def get_bs_db(self):
        con = sqlite3.connect ('sqldatabase_test.db')
        cur = con.cursor() 
        sql_sentence = 'SELECT'+ ' ' +'Blocksize'+' '+'FROM'+' '+self.Table_Names
        sql_result = cur.execute(sql_sentence)
        bs = []
        for row in sql_result:
            bs.append(row[0])
        block_size = list(set(bs))
        block_size.sort(key=bs.index)
        cur.close()
        con.commit()
        con.close()
        return block_size


    def get_rwType_db(self):
        con = sqlite3.connect ('sqldatabase_test.db')
        cur = con.cursor() 
        sql_sentence2 = 'SELECT'+ ' ' +'ReadWrite_type'+' '+'FROM'+' '+self.Table_Names
        sql_result2 = cur.execute(sql_sentence2)
        rw_ty = []
        for row in sql_result2:
            rw_ty.append(row[0])
        rw_type = list(set(rw_ty))
        rw_type.sort(key=rw_ty.index)
        cur.close()
        con.commit()
        con.close()
        return rw_type


    def get_nj_db(self):
        con = sqlite3.connect ('sqldatabase_test.db')
        cur = con.cursor() 
        sql_sentence3 = 'SELECT'+ ' ' +'Number_of_Job'+' '+'FROM'+' '+self.Table_Names
        sql_result3 = cur.execute(sql_sentence3)
        nj_l = []
        for row in sql_result3:
            nj_l.append(row[0])
        nj = list(set(nj_l))
        nj.sort(key=nj_l.index)
        cur.close()
        con.commit()
        con.close()
        return nj


    def get_IOdepth_db(self):
        con = sqlite3.connect ('sqldatabase_test.db')
        cur = con.cursor() 
        sql_sentence4 = 'SELECT'+ ' ' +'IOdepth'+' '+'FROM'+' '+self.Table_Names
        sql_result4 = cur.execute(sql_sentence4)
        IOdepth_l = []
        for row in sql_result4:
            IOdepth_l.append(row[0])
        IOdepth = list(set(IOdepth_l))
        IOdepth.sort(key=IOdepth_l.index)
        
        cur.close()
        con.commit()
        con.close()
        # print('IOddddddd',IOdepth)
        return IOdepth


    def get_device_db(self):
        con = sqlite3.connect ('sqldatabase_test.db')
        cur = con.cursor() 
        sql_sentence5 = 'SELECT'+ ' ' +'DRBD_Type'+' '+'FROM'+' '+self.Table_Names
        sql_result5 = cur.execute(sql_sentence5)
        device_l = []
        for row in sql_result5:
            device_l.append(row[0])
        device_list = list(set(device_l))
        device_list.sort(key=device_l.index)
        
        cur.close()
        con.commit()
        con.close()
        # print('IOddddddd',IOdepth)
        return device_list





class sql_graph_chart (object):
    def __init__(self,Table_Name):
        # a_yaml_file = open('sql_config.yml')
        # a = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
        # self.table_n = a['Table_Names']
        self.table_n = Table_Name
        self.y_Data = ['IOPS','MBPS']
        get_info = Get_info_db(self.table_n)
        self.blocksize_range = get_info.get_bs_db()
        self.rwType_info = get_info.get_rwType_db()
        self.nj_info = get_info.get_nj_db()
        self.IOd_info = get_info.get_IOdepth_db()
        self.draw_graph_chart()


    def draw_graph_chart(self):
        con = sqlite3.connect ('sqldatabase_test.db')
        cur = con.cursor()
        values = []
        drbd= []
        for i in range(len(self.y_Data)):
            # print('iiiii',self.y_Data[i])
            for rw in range(len(self.rwType_info)):
                # print('rrrrwww',self.rwType_info[rw])
                rwType_info ='"{}"'.format(self.rwType_info[rw])
                for nj in range(len(self.nj_info)):
                    nj_info ='"{}"'.format(self.nj_info[nj])
                    # print('njjjjjjj',self.nj_info[nj])
                    for io in range(len(self.IOd_info)): 
                        # print('ioddddddd',self.IOd_info[io])
                        IOd_info ='"{}"'.format(self.IOd_info[io])
                        sql_sentence = 'SELECT'+ ' ' +'DRBD_Type,'+self.y_Data[i]+' '+'FROM'+' '+self.table_n +' '+'WHERE'+' '+ 'Readwrite_type=' + rwType_info+ ' ' + 'AND' + ' ' + 'Number_of_Job =' + nj_info + ' ' + 'AND' + ' '+ 'IOdepth =' + IOd_info
                        # print('sssqqqll',sql_sentence)
                        sql_result = cur.execute(sql_sentence)
                        # print('aaaaa',sql_result)
                        for row in sql_result:
                            values.append(row[1])
                            drbd.append(row[0])


        # print('vvvvvvv',values)
        length = len(self.blocksize_range)
        number_of_drbd = len(values) // length
        # print ('vvvv',values)
        # print ('dddd',drbd)
        print ('nnnnnnn',number_of_drbd)
        
        values2 = []
        drbd_type = []
        for i in range(number_of_drbd):
            values2.append(values[:length])
            values = values[length:]
            drbd_type.append(drbd[length*i])
        # print ('vvvvvv2',values2)
        # print ('dtdtdtdt',drbd_type)

        plt.figure(figsize=(20,20), dpi = 100)
        plt.xlabel ('Block Size')
        ylabel = ''

        drbd_t = list(set(drbd_type))
        drbd_t.sort(key=drbd_type.index)
        n = len(drbd_t)
        num_of_picture = int(number_of_drbd / n)
        print('nnnnnn',num_of_picture)
        IO_MB_value = num_of_picture / 2

        flag = 0

        for b in [values2[i:i + n] for i in range(0, len(values2), n)]:
                # print('llll',b)
                if num_of_picture <= IO_MB_value:
                    ylabel = 'MBPS'
                else:
                    ylabel = 'IOPS'
                num_of_picture = num_of_picture -1
                plt.title(self.table_n + '-' + ylabel + '-' + self.rwType_info[flag])
                print('ttttttt',self.table_n + '-' + ylabel + '-' + self.rwType_info[flag])
                file_name = self.table_n+ '-' + ylabel + '-' + self.rwType_info[flag]

                x = self.blocksize_range
                for yy in range(len(b)):
                    plt.plot(x,b[yy],label = drbd_type[yy])

                flag +=1
                if flag == len(self.rwType_info):
                    flag = 0 
                plt.legend()
                plt.grid()
                plt.savefig(file_name)
                # plt.show()  
                plt.draw()
                plt.close(1)

        cur.close()
        con.commit()
        con.close()






def sql_graph_output():

    con = sqlite3.connect ('sqldatabase_test.db') # create connection object and database file
    cur = con.cursor() # create a cursor for connection object

    a_yaml_file = open('sql_config.yml')
    a = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
    sql_sentence = 'SELECT'+ ' ' +'DRBD_Type,'+a['Select_Data']+' '+'FROM'+' '+a['Table_Names'] +' '+'WHERE'+' '+ 'Readwrite_type =' + a['ReadWrite_Type'] + ' ' + 'AND' + ' ' + 'Number_of_Job =' + a['Number_of_Job'] + 'AND' + ' '+ 'IOdepth =' + a['IOdepth']
    sql_result = cur.execute(sql_sentence)

    blocksize_range = ['1k', '2k','4k','8k','16k','32k','64k','128k','256k','512k','1M','2M']
    
    values = []
    drbd= []
    
    for row in sql_result:
        values.append(row[1])
        drbd.append(row[0])
    # print (drbd_type)
    
    number_of_drbd = len(values) // 12
    # print (number_of_drbd)
    
    values2 = []
    drbd_type = []
    for i in range(number_of_drbd):
        values2.append(values[:12])
        values = values[12:]
        drbd_type.append(drbd[12*i])
    print (values2)

    plt.figure(figsize=(20,20), dpi = 100)
    plt.xlabel ('Block Size')
    plt.ylabel (a['Select_Data'])
    # plt.ylabel (f'{Select_Data}')
    
    for j in range(number_of_drbd):
        x = blocksize_range
        y = values2[j]
        plt.plot(x,y, label = drbd_type[j])
    
    plt.title(a['Table_Names'] + '-' + a['Select_Data'] + '-' + a['ReadWrite_Type'])
    # plt.title(f"{Table_Names}-{ReadWrite_Type}-{Select_Data}")
    plt.legend()
    plt.grid()

    file_name = a['Table_Names'] + '-' + a['ReadWrite_Type'] + '-' + a['Select_Data'] + ' ' + 'chart'
    plt.savefig(file_name)
    plt.show()

    cur.close()
    con.commit()
    con.close()



if __name__ == '__main__':
    # sql_print_index()
    # sql_graph_output()
    # a_yaml_file = open('sql_config.yml')
    # a = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
    # cc = a['Table_Names']
    # Get_info_db(cc)
    sql_graph_chart()







