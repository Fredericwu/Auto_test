# coding:utf-8
import yaml
import itertools
import time
import subprocess

from collections import OrderedDict as Odd
try:
    import configparser as cp
except Exception:
    import ConfigParser as cp




   
class Self_defined_scenarios(object):
    def __init__(self,config_file):
        self.config_file = config_file
        self.cfg = self.read_config_file()
  

    def read_config_file(self): 
        a_yaml_file = open(self.config_file)
        all_config = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
        return all_config


    def perforamce_global(self): 
        Global_str=""
        Get_Global=self.cfg.get('PerformanceGlobal')
        Get_Global_1= Get_Global.split()
        for i in range(len(Get_Global_1)):
            Global_str = Global_str + Get_Global_1[i] + '\n'
        return Global_str 


    def perforamce_setting(self):
        perforamce_setting=self.cfg.get('PerformanceSetting')
        return perforamce_setting





class Seq_rw_scenarios(object):
    def __init__(self,config_file):
        self.config_file = config_file
        self.cfg = self.read_config_file()
  

    def read_config_file(self): 
        a_yaml_file = open(self.config_file)
        all_config = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
        return all_config


    def perforamce_global(self): 
        Run_time = str(self.cfg.get('run_time'))
        size = self.cfg.get('size')
        Global_str = 'ioengine=libaio' + '\n' + 'direct=1' + '\n' + 'ramp_time=10' + '\n' + 'runtime=' + Run_time + '\n' + 'size=' + size + '\n' + 'group_reporting' + '\n' + 'new_group' + '\n'
        return Global_str 


    def perforamce_setting(self):
        device_info=self.cfg.get('test_device_info')
        perforamce_setting = {'filename': device_info, 'rw': ['read', 'write'], 'bs': ['1k', '2k', '4k', '8k', '16k', '32k', '64k', '128k', '256k', '512k', '1M','2M'], 'iodepth': ['8'], 'numjobs': ['8']}
        return perforamce_setting





class Video_scenarios(object):
    def __init__(self,config_file):
        self.config_file = config_file
        self.cfg = self.read_config_file()
  

    def read_config_file(self): 
        a_yaml_file = open(self.config_file)
        all_config = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
        return all_config


    def perforamce_global(self): 
        Run_time = str(self.cfg.get('run_time'))
        size = self.cfg.get('size')
        Global_str = 'ioengine=libaio' + '\n' + 'direct=1' + '\n' + 'ramp_time=10' + '\n' + 'runtime=' + Run_time + '\n' + 'size=' + size + '\n' + 'group_reporting' + '\n' + 'new_group' + '\n'
        return Global_str 


    def perforamce_setting(self):
        device_info=self.cfg.get('test_device_info')
        perforamce_setting = {'filename': device_info, 'rw': ['read', 'write'], 'bs': ['64k', '128k', '256k', '512k', '1M'], 'iodepth': ['8'], 'numjobs': ['1']}
        return perforamce_setting





class Random_rw_scenarios(object):
    def __init__(self,config_file):
        self.config_file = config_file
        self.cfg = self.read_config_file()


    def read_config_file(self): 
        a_yaml_file = open(self.config_file)
        all_config = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
        return all_config


    def perforamce_global(self): 
        Run_time = str(self.cfg.get('run_time'))
        size = self.cfg.get('size')
        Global_str = 'ioengine=libaio' + '\n' + 'direct=1' + '\n' + 'ramp_time=10' + '\n' + 'runtime=' + Run_time + '\n' + 'size=' + size + '\n' + 'group_reporting' + '\n' + 'new_group' + '\n' + 'rwmixread=75' + '\n'
        return Global_str 


    def perforamce_setting(self):
        device_info=self.cfg.get('test_device_info')
        perforamce_setting = {'filename': device_info, 'rw': ['randrw'], 'bs': ['1k', '2k', '4k', '8k', '16k', '32k', '64k'], 'iodepth': ['8'], 'numjobs': ['8']}
        return perforamce_setting





class Handle_performance_data():
    def __init__(self,dict_data):
        self.dict_data=dict_data


    def global_setting(self):
        self.list_value = []
        self.list_key = []
        list_value_all = []
        self.perforamce_setting_list=[]

        for key, value in sorted(self.dict_data.items()):
            if key=="filename" or key=="directory":
                self.list_key.insert(0,key)
                self.list_value.insert(0,self.dict_data[key])

            elif key=="rw":
                self.list_key.insert(1,key)
                self.list_value.insert(1,self.dict_data[key])

            else:
                self.list_key.append(key)
                self.list_value.append(value)

        for i in itertools.product(*self.list_value, repeat=1):
            list_value_all.append(i)
        for i in range(len(list_value_all)):
            dict_aaa=dict(zip(self.list_key,list_value_all[i]))
            self.perforamce_setting_list.append(dict_aaa)
        # print('pppppsssllll',self.perforamce_setting_list)
        return self.perforamce_setting_list


    def file_name(self):
        self.name_list=[]
        for dict_l in self.perforamce_setting_list:
            dict_temp = dict_l.copy()
            if 'filename' in dict_l.keys():
                path = dict_l['filename'].split('/')[-1]
                dict_temp.update({'filename':path})
            elif 'directory' in dict_l.keys():
                path = dict_l['directory'].split('/')[-1]
                dict_temp.update({'directory':path})
            name = '_'.join(dict_temp.values())
            self.name_list.append(name)
        return self.name_list





class Run_performance_case():
    def __init__(self,results_name,global_str,perforamce_setting_list,name_list):
        self.results_name = results_name
        self.global_str = global_str
        self.perforamce_setting_list = perforamce_setting_list
        self.name_list = name_list
        self.create_fio_file()
        self.run_fio()


    def create_fio_file(self):
        str_setting_list_all=[]
        str_setting_list=[]
        for i in range(len(self.perforamce_setting_list)):
            for j,k  in zip(self.perforamce_setting_list[i].keys(),self.perforamce_setting_list[i].values()):
                str_setting='{}={}'.format(j,k)
                str_setting_list_all.append(str_setting)
        count=len(self.perforamce_setting_list[i].keys())
        for i in range(0,len(str_setting_list_all),count):
            str_setting_list.append(str_setting_list_all[i:i+count])

        length=len(self.name_list)
        for index in range(length):
            fio_file=open(self.name_list[index]+'.fio','w')
            str_to_file='\n'.join(str_setting_list[index])
            fio_file.write('[global]'+'\n'+self.global_str+'\n'+'['+self.name_list[index]+']'+'\n'+str_to_file+'\n')
        fio_file.close()
        return True


    def clear_copy(self):
        command='rm -f /drbd_test/*'
        subprocess.call (command,shell=True)


    def run_fio(self):
        print("running fio,it would be take a long time...")
        for name_l in range(len(self.name_list)):
            self.clear_copy()
            fio_command="fio"+" "+self.name_list[name_l]+".fio"+" "+">>"+" "+self.results_name
            print('Running command:',fio_command)
            subprocess.call (fio_command,shell=True)





class Get_draw_info(object):
    def __init__(self,config_name):
        self.config_name = config_name
        self.cfg = self.read_config_file()
  

    def read_config_file(self): 
        a_yaml_file = open(self.config_name)
        all_config = yaml.load(a_yaml_file, Loader = yaml.FullLoader)
        return all_config

    def UID_info(self): 
        UID = self.cfg.get('unique_ID')
        return UID 


    def draw_info(self):
        draw_list = []
        excel_info = self.cfg.get('write_to_excel')
        chart_info = self.cfg.get('line_chart')
        histogram_info = self.cfg.get('histogram')
        draw_list.append(excel_info)
        draw_list.append(chart_info)
        draw_list.append(histogram_info)
        return draw_list
 



if __name__ == '__main__':
    pass
    



