import subprocess
import signal
from contextlib import contextmanager
import matplotlib.pyplot as plt
import numpy as np
from textwrap import wrap
import fnmatch
from concurrent.futures import ThreadPoolExecutor
import os
from configparser import ConfigParser, ExtendedInterpolation
import time
from datetime import datetime
import shutil

import logging

global stat_data
global before_size
global after_size
global file_names_list
global default_stats_header

file_names_list=[]
before_size=[]
after_size=[]

stat_data=""
#default_main_dir="C://Users\TorabinejadM\Desktop\Thesis\implementation//sdm-tib-github//rml-normalizer//experiments"
default_main_dir="."
default_map_folder=".//app//experiments//mappings//"
default_dataset_folder=".//app//experiments//csv//"
default_output_folder=".//app//experiments//graph//"
default_config_folder=".//app//experiments//rdfizer//"
default_stats_header="NAME,RDFIZER,STARTED_AT,EXECUTION_TIME,TOTAL_SIZE,ERROR"
default_stats_filename="all_stats.csv"
#used only for SDM-rdfizer
default_config_filename="configfile.ini"
#default_main_dir_var="${default:main_directory}"

mapping_name="mapping"
#rdfizers=["rocketrml","sdm-rdfizer","rmlmapper"]
rdfizers=["sdm-rdfizer"]

#used only for SDM-rdfizer
config_file=".//app//experiments//sdm-rdfizer//rdfizer//"+default_config_filename

dt_format='%d.%m.%Y %H:%M:%S,%f'
v_csv_output_path = "C:\\Users\\TorabinejadM\\Desktop\\Thesis\\implementation\\sdm-tib-github\\rml-normalizer\\experiments\\csv\\"
v_mapping_output_path="C:\\Users\\TorabinejadM\\Desktop\\Thesis\\implementation\\sdm-tib-github\\rml-normalizer\\experiments\\mappings\\"
v_main_dir=".//app//experiments//"

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()
fileLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)

def log_calls(function_name,start_time,type=""):
    if type!="new_run":
        if start_time is None:
            rootLogger.info("")
            dt_now = datetime.now().strftime(dt_format)
            v_start_time=time.time()
            rootLogger.info("====================== "+function_name+" started at "+dt_now+"======================")
            return v_start_time
        else:
            dt_now = datetime.now().strftime(dt_format)
            v_elpased_time=time.time()-start_time
            rootLogger.info("execuion time: {} seconds".format(v_elpased_time))
            rootLogger.info("====================== "+function_name+" finished at "+dt_now+"======================")
            rootLogger.info("")
            return v_elpased_time
    else:
        if start_time is None:
            rootLogger.info("")
            rootLogger.info("")
            rootLogger.info("")
            dt_now = datetime.now().strftime(dt_format)
            v_start_time=time.time()
            rootLogger.info("%%%%%%%%%%%%%%%%%% "+function_name+" started at "+dt_now+" %%%%%%%%%%%%%%%%%%")
            return v_start_time
        else:
            dt_now = datetime.now().strftime(dt_format)
            v_elpased_time=time.time()-start_time
            rootLogger.info("execuion time: {} seconds".format(v_elpased_time))
            rootLogger.info("%%%%%%%%%%%%%%%%%% "+function_name+" finished at "+dt_now+" %%%%%%%%%%%%%%%%%%")
            return v_elpased_time

def log_error(function_name,error_msg):
    rootLogger.setLevel(logging.ERROR)
    rootLogger.error("Error occured in "+function_name)
    rootLogger.error("Error !!!!!!!!!")
    rootLogger.error(error_msg)
    rootLogger.error("Error !!!!!!!!!")
    rootLogger.setLevel(logging.INFO)
    rootLogger.info("!!!!!!!!!!!!!!!!!!!!! "+function_name+" failed !!!!!!!!!!!!!!!!!!!!!")

def install_tools():
    try:
        start_time=None
        start_time=log_calls("install_tools",start_time)
        if os.path.exists(".//app//experiments//rmlmapper")==True:
            pass
        else:
            os.system("git clone https://github.com/RMLio/rmlmapper-java.git .//app//experiments//rmlmapper")
            pom=[]
            with open(".//app//experiments//rmlmapper//pom.xml", 'r') as f_read:
                pom=f_read.readlines()
                pom_bkp=pom[:]
                pom_finalname=list(filter(lambda x: "<finalName>" in x, pom))
                pom_line=pom.index(pom_finalname[0])
                pom_finalname_value=pom[pom_line].strip()
                pom[pom_line]=pom[pom_line].replace(pom_finalname_value,"<finalName>rmlmapper</finalName>")
            f_write=open(".//app//experiments//rmlmapper//pom.xml", 'w') 
            f_write.writelines(pom)
            f_write.close()
            os.system("mvn clean install -DskipTests -f .//app//experiments//rmlmapper")
            f_write=open(".//app//experiments//rmlmapper//pom.xml", 'w') 
            f_write.writelines(pom_bkp)
            f_write.close()
        
        if os.path.exists(".//app//experiments//rocket-rml")==True:
            pass
        else:
            os.mkdir(".//app//experiments//rocket-rml")
            shutil.copy(".//app//experiments//index.js",".//app//experiments//rocket-rml//index.js")
            os.system("npm init -y")
            shutil.copy("package.json",".//app//experiments//rocket-rml//package.json")
            os.remove("package.json")
            os.system("npm i rocketrml")
            shutil.copytree("node_modules", ".//app//experiments//rocket-rml//node_modules", False, None)
            shutil.rmtree("node_modules")
        
        if os.path.exists(".//app//experiments//sdm-rdfizer")==True:
            pass
        else:
            os.system("git clone https://github.com/SDM-TIB/SDM-RDFizer.git .//app//experiments//sdm-rdfizer")
        log_calls("install_tools",start_time)
    except Exception as e:
        log_error("install_tools",e)


def call_tools(entry,rdfizer_name,start_time):
    try:
        proc=None
        elpased_time=None
        mapping_name=entry
        mapping_file=default_map_folder+mapping_name
        call_args=[]
        output_name=str(os.path.splitext(entry)[0])+"-"+rdfizer_name
        output_path=default_output_folder
        output_file=output_path+output_name+".nt"
        
        if rdfizer_name=="sdm-rdfizer":
            config = ConfigParser(interpolation=ExtendedInterpolation())
            config.read(config_file)
            config.set("datasets", "output_folder",output_path)
            config.set("datasets", "number_of_datasets","1")
            config.set("datasets", "name",mapping_name)
            config.set("dataset1", "name",output_name)
            config.set("dataset1", "mapping",mapping_file)
            with open(config_file, 'w') as configfile:
                config.write(configfile)
            call_args=["python3",".//app//experiments//sdm-rdfizer//rdfizer//run_rdfizer.py",config_file]
        elif rdfizer_name=="rmlmapper":
            v_arguments="-m "+mapping_file+" -o "+output_file + " -d"
            call_args=['java', '-jar', './/app//experiments//rmlmapper//target//rmlmapper.jar','-m',mapping_file,'-o',output_file,'-d']
        elif rdfizer_name=="rocketrml":
            call_args=["node",".//app//experiments//rocket-rml//index.js",mapping_file,output_file]
        
        start_time=time.time()
        
        start_time=time.time()
        proc = subprocess.Popen(call_args,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        proc.wait()
        elpased_time=time.time()-start_time
        err_flag=0
        while True:
            line_out = proc.stdout.readline()
            line_err = proc.stderr.readline()
            if line_out:
                rootLogger.info(str(line_out.rstrip()))
            elif line_err:
                rootLogger.setLevel(logging.ERROR)
                rootLogger.error(str(line_err.rstrip()))
                rootLogger.setLevel(logging.INFO)
            else:
                break
        
        proc.stdout.close()
        proc.stderr.close()
        if err_flag==1:
            raise
        #To remove extra files generated by SDM-rdfizer
        if rdfizer_name=="sdm-rdfizer":
            os.remove(default_output_folder+"stats.csv")
            os.remove(default_output_folder+mapping_name+"_datasets_stats.csv")
        return elpased_time
    except:
        if proc!=None:
            proc.stdout.close()
            proc.stderr.close()
            proc.kill()
        raise
        #raise Exception("call_tools failed!!!")
def run_experiments():
    try:
        main_start_time=None
        main_start_time=log_calls("run_experiments",main_start_time,"new_run")
        global stat_data
        global before_size
        global after_size
        global file_names_list
        global default_stats_header
        v_elpased_time=None
        
        bar_names=[]
        
        install_tools()
        
        try:
            with open(default_output_folder+default_stats_filename, 'r') as f_in:
                default_stats_header=f_in.readline()
        except FileNotFoundError as e:
            with open(default_output_folder+default_stats_filename, 'a+',newline="") as f_out:
                f_out.write(default_stats_header+"\n")
        
        config = ConfigParser(interpolation=ExtendedInterpolation())
        config.read(".//app//experiments//configfile.ini")
        number_of_rules = config.getint("rules","number_of_rules")
        
        entries = os.listdir(default_map_folder)
        print(entries)
        DIR=default_map_folder
        for i in range(0,int(number_of_rules)):
            entry = config["rule"+str(i+1)]["filename"]
            total_size=0
            ds_list = config["rule"+str(i+1)]["dataset_name"].split(",")
            if entry.endswith('.ttl'):
                if entry.endswith('_normal.ttl'):
                    for csv_file in(ds_list):
                        file_stats = os.stat(default_dataset_folder+csv_file)
                        total_size=total_size+file_stats.st_size
                else:
                    file_stats = os.stat(default_dataset_folder+os.path.splitext(entry)[0]+'.csv')
                    total_size=total_size+file_stats.st_size
                total_size=total_size/ (1024*1024)
                
                for i in rdfizers:
                    file_names_list.append(i)
                    dt_now = datetime.now().strftime(dt_format)
                    start_time=time.time()
                    
                    signal.signal(signal.SIGALRM, raise_timeout)
                    alarm_time=7200
                    signal.alarm(alarm_time) #3 hours
                    message=""
                    start_time=None
                    try:
                        start_time=log_calls(i+" rdfizing dataset "+entry,start_time)
                        v_elpased_time=call_tools(entry,i,start_time=dt_now)
                        message=None
                    except TimeoutError:
                        v_elpased_time=alarm_time
                        message="timed_out"
                        log_error(i+" rdfizing dataset "+entry,'timed_out') 
                        pass
                    except Exception as e:
                        print("here")
                        v_elpased_time=time.time()-start_time
                        message="other errors"
                        log_error(i+" rdfizing dataset "+entry,e) 
                        pass
                    finally:
                        log_calls(i+" rdfizing dataset "+entry,start_time)
                        signal.signal(signal.SIGALRM, signal.SIG_IGN)
                        
                    stat_data=stat_data+entry+","+i+",\""+str(dt_now)+"\","+str(v_elpased_time)+","+str(total_size)+","+str(message)+"\n"
            else:
                pass 
    
        with open(default_output_folder+default_stats_filename, 'a+',newline="") as f_out:
            f_out.write(stat_data)
            f_out.close()
        log_calls("run_experiments",main_start_time,"new_run")
    except Exception as e:
        log_error("run_experiments",e)

def raise_timeout(signum, frame):
    raise TimeoutError    
    
        
def main():
    run_experiments()


if __name__=="__main__":
    main()
