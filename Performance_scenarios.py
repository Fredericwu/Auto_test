# coding:utf-8
import yaml
import itertools
import time
import subprocess
from pathlib import Path
import os.path
import Performance_get_config as gc
import SQL_input_fred2 as sql
import sql_chart_fred as chart
import sql_histogram_new_fred as histogram
import sql_select_fred as excel
import utils
import log
import index as ind
import sqlite3

   
# time_n = time.localtime(time.time())
# Date = time.strftime('%y%m%d%H%M',time_n) 
# results_name = time.strftime('%m_%d_%H_%M',time_n)+'_'+'results.txt'  
results_name = '08_06_05_23_results.txt'
Client_Name = 'Auto_GZ'
Disk_Type = 'Auto_type'





class Check_file_dir(object):
    def __init__(self):
        # time_n = time.localtime(time.time())
        # self.Date = time.strftime('%y%m%d%H%M',time_n) 
        self.Performance_dir = Path("performance_data")
        self.Config_dir = Path("config")
        self.Sql_db_file = Path('sqldatabase_test.db')
        self.check_dir_file_fun()
        self.check_db_file()


    def check_dir_file_fun(self): 
        if self.Performance_dir.exists() == False:
            utils.prt_log('', f"Creating performance_data directory automatically", 0)
            os.mkdir(self.Performance_dir)

        if self.Config_dir.exists() == False:
            utils.prt_log('', f"Creating config directory automatically", 0)
            os.mkdir(self.Config_dir)


    def check_db_file(self): 
        if self.Sql_db_file.exists() == False:
            utils.prt_log('', f"Creating sqldatabase_test.db file automatically", 0)
            command = 'touch ' + 'sqldatabase_test.db'
            subprocess.run(command,shell=True)
            con = sqlite3.connect ('sqldatabase_test.db') 
            cur = con.cursor()
            cur.execute('''CREATE TABLE Index_Table
                            (Key_ID integer,
                            Client_Name text,
                            Date text,
                            Disk_Type text,
                            Text_Table_Name text
                            )''')
            cur.close()
            con.commit()
            con.close()





class Self_defined_case(object):
    def __init__(self,config_file):
        self.config_file =  config_file
        # self.config_file = './config/P_self_defined_scenario.yml'
        # Check_file_dir()
        time_n = time.localtime(time.time())
        # self.Date = time.strftime('%y%m%d%H%M',time_n) 
        results_name = time.strftime('%m_%d_%H_%M',time_n)+'_'+'results.txt'  
        # print('Date.............',results_name)
        self.check_yml_file()
        self.perforamce_info()
        self.run_test()
        IOd = self.per_setting.get('iodepth')[0] #only support the first iodepth value
        job = self.per_setting.get('numjobs')[0] #only support the first numjobs value
        Run_selected_function(self.config_file,self.per_setting,IOd,job )
        self.rm_fio()


    def check_yml_file(self): 
        # performance_dir = Path("performance_data")
        # if performance_dir.exists() == False:
        #     os.mkdir('performance_data')

        # if Config_dir.exists() == False:
        #     os.mkdir('config')

        config_file = Path(self.config_file)
        config_str = '''#self_defined_config yaml file
PerformanceGlobal:
  ioengine=libaio 
  direct=1 
  ramp_time=10
  runtime=360
  size=70%
  group_reporting
  new_group

#if test directory,please input size=size_capacity on PerformanceGlobal
PerformanceSetting: 
  filename: [/dev/sdx,/dev/sdxx]
  bs: [1k,2k,4k,8k,16k,32k,64k,128k,256k,512k,1M,2M]
  rw: [read,write,randread,randwrite]
  iodepth: ['8']
  numjobs: ['8']

run_count: 1
unique_ID: 10000
write_to_excel: False
line_chart: False
histogram: False
        '''

        if config_file.exists() == False:
            file = open(config_file,'w')
            file.write(config_str)
            file.close()
            utils.prt_log('', f"Creating P_self_defined_scenario.yml file automatically", 0)


    def perforamce_info(self): 
        Performance_config = gc.Self_defined_scenarios(self.config_file)
        self.Global_str = Performance_config.perforamce_global()
        self.per_setting = Performance_config.perforamce_setting()
        # print('pppppsssss',self.per_setting)

        Performance_data = gc.Handle_performance_data(self.per_setting)
        self.perforamce_setting_list = Performance_data.global_setting()
        self.name_list = Performance_data.file_name()


    def run_test(self):
        test = gc.Run_performance_case(results_name,self.Global_str,self.perforamce_setting_list,self.name_list)


    def rm_fio(self):
        for i in range(len(self.name_list)):
            command = 'rm ' + self.name_list[i]+'.fio'
            subprocess.run(command,shell=True)
        utils.prt_log('', f"Remove all .fio file automatically after test", 0)



class Seq_rw_case(object):
    def __init__(self,config_file):
        self.config_file = config_file
        time_n = time.localtime(time.time())
        # self.Date = time.strftime('%y%m%d%H%M',time_n) 
        results_name = time.strftime('%m_%d_%H_%M',time_n)+'_'+'results.txt'  
        self.check_yml_file()
        self.perforamce_info()
        self.run_test()
        IOd = self.per_setting.get('iodepth')[0]
        job = self.per_setting.get('numjobs')[0]
        Run_selected_function(self.config_file,self.per_setting,IOd,job)
        self.rm_fio()


    def check_yml_file(self): 

        config_file = Path(self.config_file)
        config_str = '''#sequential read/write test
run_count: 1
run_time: 300
test_device_info: [/dev/sdxx,/dev/sdxxx] 
size: 70%

unique_ID: 20000
write_to_excel: False
line_chart: False
histogram: False

        '''

        if config_file.exists() == False:
            file = open(config_file,'w')
            file.write(config_str)
            file.close()
            utils.prt_log('', f"Creating P_seq_rw_scenario.yml file automatically", 0)


    def perforamce_info(self): 
        Performance_config = gc.Seq_rw_scenarios(self.config_file)
        self.Global_str = Performance_config.perforamce_global()
        self.per_setting = Performance_config.perforamce_setting()
        Performance_data = gc.Handle_performance_data(self.per_setting)
        self.perforamce_setting_list = Performance_data.global_setting()
        # print('pppppp',self.perforamce_setting_list)
        self.name_list = Performance_data.file_name()


    def run_test(self):
        test = gc.Run_performance_case(results_name,self.Global_str,self.perforamce_setting_list,self.name_list)


    def rm_fio(self):
        for i in range(len(self.name_list)):
            command = 'rm ' + self.name_list[i]+'.fio'
            subprocess.run(command,shell=True)
        utils.prt_log('', f"Remove all .fio file automatically after test", 0)


class Video_scenarios_case(object):
    def __init__(self,config_file):
        self.config_file = config_file
        time_n = time.localtime(time.time())
        # self.Date = time.strftime('%y%m%d%H%M',time_n) 
        results_name = time.strftime('%m_%d_%H_%M',time_n)+'_'+'results.txt' 
        self.check_yml_file()
        self.perforamce_info()
        self.run_test()
        IOd = self.per_setting.get('iodepth')[0]
        job = self.per_setting.get('numjobs')[0]
        Run_selected_function(self.config_file,self.per_setting,IOd,job)
        self.rm_fio()


    def check_yml_file(self): 

        config_file = Path(self.config_file)
        config_str = '''#Video scenario read/write test

run_count: 1
run_time: 300
test_device_info: [/dev/sdxx,/dev/sdxxx] 
size: 70%

unique_ID: 30000
write_to_excel: False
line_chart: False
histogram: False

        '''

        if config_file.exists() == False:
            file = open(config_file,'w')
            file.write(config_str)
            file.close()
            utils.prt_log('', f"Creating P_video_scenario.yml file automatically", 0)


    def perforamce_info(self): 
        Performance_config = gc.Video_scenarios(self.config_file)
        self.Global_str = Performance_config.perforamce_global()
        self.per_setting = Performance_config.perforamce_setting()
        Performance_data = gc.Handle_performance_data(self.per_setting)
        self.perforamce_setting_list = Performance_data.global_setting()
        self.name_list = Performance_data.file_name()


    def run_test(self):
        test = gc.Run_performance_case(results_name,self.Global_str,self.perforamce_setting_list,self.name_list)
        test.create_fio_file()
        test.run_fio()


    def rm_fio(self):
        for i in range(len(self.name_list)):
            command = 'rm ' + self.name_list[i]+'.fio'
            subprocess.run(command,shell=True)
        utils.prt_log('', f"Remove all .fio file automatically after test", 0)


class Random_rw_scenarios_case(object):
    def __init__(self,config_file):
        self.config_file = config_file
        time_n = time.localtime(time.time())
        # self.Date = time.strftime('%y%m%d%H%M',time_n) 
        results_name = time.strftime('%m_%d_%H_%M',time_n)+'_'+'results.txt' 
        self.check_yml_file()
        self.perforamce_info()
        self.run_test()
        IOd = self.per_setting.get('iodepth')[0]
        job = self.per_setting.get('numjobs')[0]
        Run_selected_function(self.config_file,self.per_setting,IOd,job)
        self.rm_fio()


    def check_yml_file(self): 

        config_file = Path(self.config_file)
        config_str = '''#random read/write scenario test

run_count: 1
run_time: 300
test_device_info: [/dev/sdxx,/dev/sdxxx] 
size: 70%

unique_ID: 40000
write_to_excel: False
line_chart: False
histogram: False

        '''

        if config_file.exists() == False:
            file = open(config_file,'w')
            file.write(config_str)
            file.close()
            utils.prt_log('', f"Creating P_random_rw_scenario.yml file automatically", 0)


    def perforamce_info(self): 
        Performance_config = gc.Random_rw_scenarios(self.config_file)
        self.Global_str = Performance_config.perforamce_global()
        self.per_setting = Performance_config.perforamce_setting()
        Performance_data = gc.Handle_performance_data(self.per_setting)
        self.perforamce_setting_list = Performance_data.global_setting()
        self.name_list = Performance_data.file_name()


    def run_test(self):
        test = gc.Run_performance_case(results_name,self.Global_str,self.perforamce_setting_list,self.name_list)
        test.create_fio_file()
        test.run_fio()


    def rm_fio(self):
        for i in range(len(self.name_list)):
            command = 'rm ' + self.name_list[i]+'.fio'
            subprocess.run(command,shell=True)
        utils.prt_log('', f"Remove all .fio file automatically after test", 0)




class Run_selected_function(object):
    def __init__(self,config_file,per_setting,iodepth,job):
        time_n = time.localtime(time.time())
        # self.Date = time.strftime('%Y-%m-%d %H:%M:%S',time_n) 
        self.config_file = config_file
        self.per_setting = per_setting 
        self.iodepth = iodepth
        self.job = job
        self.function_info()
        # self.write_to_db()
        self.run_funtion()


    def function_info(self): 
        Draw_config = gc.Get_draw_info(self.config_file)
        self.UID = Draw_config.UID_info()
        self.draw_info = Draw_config.draw_info()

    def write_to_db(self): 
        handle_data = sql.Handle_data_function(results_name)
        self.list_data = handle_data.handle_mbps()
        sql.Write_to_database(self.UID,Client_Name,Date,Disk_Type,self.list_data)
        utils.prt_log('', f"Write data to sqldatabase_test.db automatically", 0)
        # print('ddddd')


    def run_funtion(self):
        Select_Data = ['IOPS','MBPS']
        # Table_Names = Client_Name + '_' + Date + '_' + Disk_Type
        Table_Names = 'GZ_20211266_Fred'
        # Performance_config = gc.Self_defined_scenarios('./config/P_self_defined_scenario.yml') #need to known scenarios
        # self.dict_data = Performance_config.perforamce_setting() #bs form theis config


        if self.draw_info[0] == True:
            # print(self.Date,'[INFO] Writing data to excel.....')
            get_info = chart.Get_info_db(Table_Names)
            rwType_info = get_info.get_rwType_db()
            exc = excel.sql_write_excel(Table_Names,rwType_info)
            exc.write_to_excel_all()
            utils.prt_log('', f"Write all data to excel one table automatically , Done", 0)

            for i in range(len(rwType_info)):
                exc.get_data_IO_MB(rwType_info[i])    
                exc.write_to_excel_bs(self.per_setting,rwType_info[i],self.iodepth,self.job)
            utils.prt_log('', f"Write all data to excel bs table automatically , Done", 0)

        else:
            # print(self.Date,'[INFO] Not write to excel.....')
            utils.prt_log('', f"Not write to excel.....", 0)

        if self.draw_info[1] == True:
            # print(self.Date,'[INFO] Drawing line chart.....')
            chart.sql_graph_chart(Table_Names)
            utils.prt_log('', f"Drawing line chart , Done", 0)
        else:
            # print(self.Date,'[INFO] Not draw line chart.....')
            utils.prt_log('', f"Not draw line chart.....", 0)
            
        if self.draw_info[2] == True:
            # print(self.Date,'[INFO] Drawing histogram picture.....')
            utils.prt_log('', f"Drawing histogram picture", 0)
            IOPS_4K_list = ['IOPS','4k']
            MBPS_1M_list = ['MBPS','1M']
            histogram_run = histogram.sql_graph_histogram(Table_Names)
            fn_l,val=histogram_run.get_data_histogram(IOPS_4K_list)
            histogram_run.draw_graph_histogram(fn_l,val)
            utils.prt_log('', f"Draw IOPS histogram picture , Done", 0)

            fn_l2,val2=histogram_run.get_data_histogram(MBPS_1M_list)
            histogram_run.draw_graph_histogram(fn_l2,val2)
            utils.prt_log('', f"Draw MBPS histogram picture , Done", 0)

        else:
            # print(self.Date,'[INFO] Not draw histogram picture.....')
            utils.prt_log('', f"Not draw histogram picture.....", 0)




# class Get_sys_info(object):
#     def __init__(self):
#         time_n = time.localtime(time.time())
#         self.install_soft()
        

#     def install_soft(self): 
#         command = ['apt install -y htop atop sysstat','apt install -y aha','apt install -y html2text']
#         for i in range(len(command)):
#             try:
#                 check = subprocess.run(command[i],shell=True)
#                 check_code = check.check_returncode()
#                 utils.prt_log('', f"install htop or atop or aha or html2text software , Done", 0)
#             except:
#                 utils.prt_log('', f"htop or atop or aha or html2text software install failed", 2)


#     def get_htop_info(self): 
#         try:
#             command1 = 'echo q | htop -C | aha --line-fix | html2text -width 999 | grep -v "F1Help" | grep -v "xml version=" > ./performance_data/sysinfo.txt'
#             check = subprocess.run(command1,shell=True)
#             check_code = check.check_returncode()
#             utils.prt_log('', f"Write htop info to /performance_data/sysinfo.txt file , Done", 0)
#         except:
#             utils.prt_log('', f"htop command not found or aha/html2text software not install", 2)


#     def get_atop_info(self): 
#         try:
#             command1 = 'atop -r | head -60  > ./performance_data/sysinfo.txt'
#             check = subprocess.run(command1,shell=True)
#             check_code = check.check_returncode()
#             utils.prt_log('', f"Write atop info to /performance_data/sysinfo.txt file , Done", 0)
#         except:
#             utils.prt_log('', f"atop command not found or software not install", 2)


#     def get_top_info(self): 
#         try:
#             command1 = 'top  -n 55 -b -d 30 | head -55 > ./performance_data/sysinfo.txt'
#             check = subprocess.run(command1,shell=True)
#             check_code = check.check_returncode()
#             utils.prt_log('', f"Write top info to /performance_data/sysinfo.txt file , Done", 0)
#         except:
#             utils.prt_log('', f"top command not found or software not install", 2)


#     def get_iostat_info(self): 
#         try:
#             command1 = 'iostat -x 1 5 /dev/* > ./performance_data/sysinfo.txt'
#             check = subprocess.run(command1,shell=True)
#             check_code = check.check_returncode()
#             utils.prt_log('', f"Write iostat info to /performance_data/sysinfo.txt file , Done", 0)
#         except:
#             utils.prt_log('', f"iostat command not found or software not install", 2)





class Control_test(object):
    def __init__(self,config_yml):
        # time_n = time.localtime(time.time())
        # self.Date = time.strftime('%y%m%d%H%M',time_n) 
        self.config_yml = './config/' + config_yml
        Check_file_dir()
        Draw_config = gc.Get_draw_info(self.config_yml)
        self.count = Draw_config.count_info()
        self.run_test()


    def run_test(self): 
        # print('counttttt',self.count)
        for i in range(self.count):
            # print('tttttttest')
            print_info = ind.sql_print_index()
            if 'self_defined' in self.config_yml:
                # print(self.Date,'[INFO]run self defined test')
                utils.prt_log('', f"run self defined test", 0)
                Self_defined_case(self.config_yml)
                utils.prt_log('', f"run self defined test , Done", 0)
                utils.prt_log('', f"\n \n \n ", 0)
                # print('Date.............',Date)
                time.sleep(30)

            if 'seq_rw' in self.config_yml:
                # print(self.Date,'[INFO]run seq rw test')
                utils.prt_log('', f"run seq rw test", 0)
                Seq_rw_case(self.config_yml)
                utils.prt_log('', f"run seq rw test , Done", 0)
                utils.prt_log('', f"\n \n \n ", 0)
                time.sleep(30)

            if 'video_scenario' in self.config_yml:
                # print(self.Date,'[INFO]run Video scenarios test')
                utils.prt_log('', f"run Video scenarios test", 0)
                Video_scenarios_case(self.config_yml)
                utils.prt_log('', f"run Video scenarios test , Done", 0)
                utils.prt_log('', f"\n \n \n ", 0)
                time.sleep(30)

            if 'random_rw' in self.config_yml:
                # print(self.Date,'[INFO]run random rw scenarios test')
                utils.prt_log('', f"run random rw scenarios test", 0)
                Random_rw_scenarios_case(self.config_yml)
                utils.prt_log('', f"run random rw scenarios test , Done", 0)
                utils.prt_log('', f"\n \n \n ", 0)
                time.sleep(10)




if __name__ == '__main__':
    # pass
    # Self_defined_case()
    # Seq_rw_case()
    # Video_scenarios_case()
    # Random_rw_scenarios_case()
    # Run_selected_function()
    utils._init()
    logger = log.Log()
    utils.set_logger(logger)
    Control_test('P_random_rw_scenario.yml')




