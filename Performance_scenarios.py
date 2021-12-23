# coding:utf-8
import yaml
import itertools
import time
import subprocess
import Performance_get_config as gc
import SQL_input_fred2 as sql
import sql_chart_fred as chart
import sql_histogram_new_fred as histogram
import sql_select_fred as excel

   
time_n = time.localtime(time.time())
Date = time.strftime('%y%m%d%H%M',time_n)
# results_name = time.strftime('%m_%d_%H_%M',time_n)+'_'+'results.txt'  
results_name = '12_10_10_59_results.log'
Client_Name = 'Auto_GZ'
Disk_Type = 'Auto_type'



class Self_defined_case(object):
    def __init__(self):
        self.config_file = './config/P_self_defined_scenario.yml'
        self.perforamce_info()
        self.run_test()
        IOd = self.per_setting.get('iodepth')[0] #only support the first iodepth value
        job = self.per_setting.get('numjobs')[0] #only support the first numjobs value
        # print('IOddddd111111',IOd)
        # self.parameter_dict =  {'filename': '\\w+', 'rw': self.rwType, 'bs': '\\w+', 'iodepth': IOd, 'numjobs': job}
        # print('pdddds11111',self.parameter_dict)
        Run_selected_function(self.config_file,self.per_setting,IOd,job )


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





class Seq_rw_case(object):
    def __init__(self):
        self.config_file = './config/P_seq_rw_scenario.yml'
        self.perforamce_info()
        self.run_test()
        IOd = self.per_setting.get('iodepth')[0]
        job = self.per_setting.get('numjobs')[0]
        Run_selected_function(self.config_file,self.per_setting,IOd,job)


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





class Video_scenarios_case(object):
    def __init__(self):
        self.config_file = './config/P_video_scenario.yml'
        self.perforamce_info()
        self.run_test()
        IOd = self.per_setting.get('iodepth')[0]
        job = self.per_setting.get('numjobs')[0]
        Run_selected_function(self.config_file,self.per_setting,IOd,job)


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



class Random_rw_scenarios_case(object):
    def __init__(self):
        self.config_file = './config/P_random_rw_scenario.yml'
        self.perforamce_info()
        self.run_test()
        IOd = self.per_setting.get('iodepth')[0]
        job = self.per_setting.get('numjobs')[0]
        Run_selected_function(self.config_file,self.per_setting,IOd,job)


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




class Run_selected_function(object):
    def __init__(self,config_file,per_setting,iodepth,job):
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
        # print('ddddd')


    def run_funtion(self):
        Select_Data = ['IOPS','MBPS']
        # Table_Names = Client_Name + '_' + Date + '_' + Disk_Type
        Table_Names = 'GZ_20211266_Fred'
        # Performance_config = gc.Self_defined_scenarios('./config/P_self_defined_scenario.yml') #need to known scenarios
        # self.dict_data = Performance_config.perforamce_setting() #bs form theis config


        if self.draw_info[0] == True:
            print('wwwriteee.....')
            get_info = chart.Get_info_db(Table_Names)
            rwType_info = get_info.get_rwType_db()
            exc = excel.sql_write_excel(Table_Names,rwType_info)
            exc.write_to_excel_all()

            for i in range(len(rwType_info)):
                exc.get_data_IO_MB(rwType_info[i])    
                exc.write_to_excel_bs(self.per_setting,rwType_info[i],self.iodepth,self.job)

        else:
            print('Not write to excel......')

        if self.draw_info[1] == True:
            print('dddddraw.....')
            # chart.sql_graph_chart(Table_Names)
        else:
            print('Not draw line chart......')
            
        if self.draw_info[2] == True:
            print('hhhhhhistogram.....')
            IOPS_4K_list = ['IOPS','4k']
            MBPS_1M_list = ['MBPS','1M']
            histogram_run = histogram.sql_graph_histogram(Table_Names)
            fn_l,val=histogram_run.get_data_histogram(IOPS_4K_list)
            histogram_run.draw_graph_histogram(fn_l,val)
            fn_l2,val2=histogram_run.get_data_histogram(MBPS_1M_list)
            histogram_run.draw_graph_histogram(fn_l2,val2)

        else:
            print('Not draw histogram picture......')




if __name__ == '__main__':
    # pass
    Self_defined_case()
    # Seq_rw_case()
    # Video_scenarios_case()
    # Random_rw_scenarios_case()
    # Run_selected_function()




