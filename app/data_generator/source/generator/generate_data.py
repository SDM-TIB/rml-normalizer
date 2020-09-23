import pandas as pd
import time
from datetime import datetime
from treelib import Node, Tree
import csv
from concurrent.futures import ThreadPoolExecutor
import random
import os
import json
import pprint
import math
from configparser import ConfigParser, ExtendedInterpolation
import numpy as np
import matplotlib.pyplot as plt
import logging
import sys

global fd_dict
global fd_config
global tree_file_name
global first_level

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.INFO)

fileHandler = logging.FileHandler("{0}/{1}.log".format(".//app//experiments//graph//", "log_"+str(datetime.now().strftime('%d_%m_%Y-%H_%M_%S'))),mode='w')
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

#change if needed
#//////////////////////////////////////////
output_file=".\\output\\"
first_level=8
first_letter=65
dt_format='%d.%m.%Y %H:%M:%S,%f'
global all_out_text
all_out_text=[]
v_last_range=5
v_output_file_index=0
#//////////////////////////////////////////
    
def update_input_config(current_level=1):
    try:
        global fd_config
        v_add_up=1
        v_distinct_values=2
        is_parent=fd_config.getboolean("level"+str(current_level),"is_parent")
        total_vals=fd_config.getint("level"+str(current_level),"total_vals")
        
        number_of_distinct_vals=fd_config.getint("level"+str(current_level),"number_of_distinct_vals")
        
        if is_parent==False and current_level+1<=first_level:
            v_add_up=v_add_up*number_of_distinct_vals*update_input_config(current_level+1)
            fd_config["level"+str(current_level)]["total_vals"]=str(v_add_up)
        elif is_parent==True:
            v_add_up=number_of_distinct_vals
            if current_level!=first_level:
                fd_config["level"+str(current_level)]["total_vals"]=str(v_add_up)
        
        return v_add_up
    except Exception as e:
        log_error("update_input_config",e)

def set_col_headers():
    try:
        start_time=None
        start_time=log_calls("set_col_headers",start_time)
        with open(output_file, 'w',newline="") as f_out:
            for i in range(1,first_level+1):
                if first_level!=i:
                    f_out.write(chr(first_letter+first_level-i)+',')
                else:
                    f_out.write(chr(first_letter+first_level-i)+'\n')
            f_out.close()
        log_calls("set_col_headers",start_time)
    except Exception as e:
        log_error("set_col_headers",e)

def generate_data(level=0,tree=Tree(),parent="",parent_index=0,out_text=[],start=1,end=3,addup=0,node_id=""):
    try:
        global all_out_text
        global output_file
        global v_output_file_index
        global fd_config
        global datasets
        global tree_file_name
        global max_tuples
        letter=first_letter+first_level-level
        if level!=0:
            
            is_parent=fd_config.getboolean("level"+str(level),"is_parent")
            if level==first_level:
                if is_parent==False:
                    
                    num_of_distinct_vals=fd_config.getint("level"+str(level),"number_of_distinct_vals")
                    for i in range(1,num_of_distinct_vals+1):
                        v_output_file_index=v_output_file_index+1
                        out_text.append(chr(letter)+str(f'{i:07}'))
                        all_out_text.append(out_text[:])
                        del out_text[-1]
                else:
                    for i in range(start,end):
                        v_output_file_index=v_output_file_index+1
                        out_text.append(chr(letter)+str(f'{i:07}'))
                        all_out_text.append(out_text[:])
                        del out_text[-1]
            elif level==1:
                number_of_dup_values=fd_config.getint("level"+str(level),"number_of_dup_values")
                number_of_values=fd_config.getint("level"+str(level),"number_of_distinct_vals")
                for i in range(1,number_of_values+1):
                    next_num_of_distinct_vals=fd_config.getint("level"+str(level+1),"number_of_distinct_vals")
                    
                    if i>number_of_dup_values:
                        start_point=(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)
                        end_point=(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)+1
                    else:
                        start_point=(i-1)*next_num_of_distinct_vals+1
                        end_point=(i)*next_num_of_distinct_vals+1
                    out_text.append(chr(letter)+str(f'{i:07}'))
                    generate_data(level+1,tree,parent=node_id,parent_index=i,out_text=out_text,start=start_point,end=end_point,node_id=node_id)
                    del out_text[-1]
            else:
                if is_parent==False:
                    
                    num_of_distinct_vals=fd_config.getint("level"+str(level),"number_of_distinct_vals")
                    next_num_of_distinct_vals=fd_config.getint("level"+str(level+1),"number_of_distinct_vals")
                    number_of_dup_values=fd_config.getint("level"+str(level),"number_of_dup_values")
                    
                    for i in range(1,num_of_distinct_vals+1):
                        
                        total_vals=fd_config.getint("level"+str(level),"total_vals")
                        v_addup=addup+(parent_index-1)*total_vals
                        
                        not_dup_factor=parent_index-1
                        if i>number_of_dup_values:
                            start_point=v_addup+(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                            end_point=v_addup+(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)+1+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                        else:
                            start_point=v_addup+(i-1)*next_num_of_distinct_vals+1+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                            end_point=v_addup+i*next_num_of_distinct_vals+1+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                        out_text.append(chr(letter)+str(f'{i:07}'))
                        generate_data(level+1,tree,parent=node_id,parent_index=i,out_text=out_text,start=start_point,end=end_point,addup=v_addup,node_id=node_id)
                        del out_text[-1]
                else:
                    to_be_duplicated_cnt=0
                    for i in range(start,end):
                        to_be_duplicated_cnt=to_be_duplicated_cnt+1
                        num_of_distinct_vals=fd_config.getint("level"+str(level),"number_of_distinct_vals")
                        next_num_of_distinct_vals=fd_config.getint("level"+str(level+1),"number_of_distinct_vals")
                        number_of_dup_values=fd_config.getint("level"+str(level),"number_of_dup_values")
                        
                        if level==first_level-1:
                            diff=fd_config.getint("level"+str(first_level),"total_vals_diff")
                            if i>number_of_dup_values-diff and diff>0: #this condition is for considering the last N portion of the data instead of first ones.
                                next_num_of_distinct_vals=next_num_of_distinct_vals+1
                        
                        not_dup_factor=parent_index-1
                        if i>number_of_dup_values:
                            start_point=(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)
                            end_point=(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)+1
                        else:
                            start_point=(i-1)*next_num_of_distinct_vals+1
                            end_point=i*next_num_of_distinct_vals+1
                        out_text.append(chr(letter)+str(f'{i:07}'))
                        generate_data(level+1,tree,parent=node_id,parent_index=i,out_text=out_text,start=start_point,end=end_point,addup=0,node_id=node_id)
                        del out_text[-1]
        else:
            v_output_file_index=0
            
            all_out_text=[]
            generate_data(level+1,tree,parent="root")
            rootLogger.info(str(v_output_file_index)+" tuples has been generated!")
            output_file=output_file+tree_file_name+".csv"
            try:
                os.remove(output_file)
            except FileNotFoundError:
                pass
            set_col_headers()
            
            ###########Using normal I/O######################
            with open(output_file, 'a+',newline='') as f_out:
                writer = csv.writer(f_out)
                writer.writerows(all_out_text)
                f_out.close()
            ###########Using normal I/O######################
            
            #print(tree.show())
    except Exception as e:
        log_error("generate_data",e)

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
        
def fill_difference():
    try:
        expected_total_tuples=fd_config.getint("level"+str(first_level),"total_vals")
        actual_total_tuples=fd_config.getint("level"+str(first_level),"number_of_dup_values")
        diff_val=expected_total_tuples-actual_total_tuples
        fd_config["level"+str(first_level)]["total_vals_diff"]=str(diff_val)
        if diff_val<0:
            fd_config["level"+str(first_level-1)]["number_of_dup_values"]=str(fd_config.getint("level"+str(first_level-1),"number_of_dup_values") + diff_val)
    except Exception as e:
        log_error("fill_difference",e)
        
def run_generation(number_of_datasets=1,output_path=".\\output\\"):
    try:
        default_stats_header="NAME,STARTED_AT,EXECUTION_TIME"
        try:
            with open(".//app//experiments//graph//data_generation_times.csv", 'r') as f_in:
                default_stats_header=f_in.readline()
        except FileNotFoundError as e:
            with open(".//app//experiments//graph//data_generation_times.csv", 'a+',newline="") as f_out:
                f_out.write(default_stats_header+"\n")
        main_start_time=None
        main_start_time=log_calls("run_generation",main_start_time,'new_run')
        
        global output_file
        global fd_config
        global datasets
        global tree_file_name
        global max_tuples
        global first_level
        elpased_time=0
        stat_data="\n"
        
        datasets = ConfigParser(interpolation=ExtendedInterpolation())
        datasets.read(".//app//data_generator//source//generator//configfile.ini")
        number_of_datasets=datasets.getint("datasets","number_of_datasets")
        
        rules = ConfigParser(interpolation=ExtendedInterpolation())
        rules.read(".//app//source//configfile.ini")
        rules["default"]["name_of_datasets"]=""



        for i in range(1,number_of_datasets+1):
            dt_now = datetime.now().strftime(dt_format)
            output_file=output_path
            tree_file=datasets["dataset"+str(i)]["tree_file"]
            first_level=int(datasets["dataset"+str(i)]["number_of_levels"])
            fd_config = ConfigParser(interpolation=ExtendedInterpolation())
            fd_config.read(tree_file)
            
            tree_file_name=os.path.splitext((os.path.basename(tree_file)))[0]
            
            rules["default"]["name_of_datasets"]=rules["default"]["name_of_datasets"]+str(tree_file_name+".csv")+","
            
            start_time=None
            start_time=log_calls("update_input_config",start_time)
            for i in range(1,first_level+1):
                update_input_config(i)
            log_calls("update_input_config",start_time)
            
            start_time=None
            start_time=log_calls("fill_difference",start_time)
            fill_difference()
            log_calls("fill_difference",start_time)
            
            with open(tree_file, 'w') as configfile:
                fd_config.write(configfile)
            
            start_time=None
            start_time=log_calls("generate_data",start_time)
            generate_data()
            elpased_time=log_calls("generate_data",start_time)
            stat_data=stat_data+os.path.basename(tree_file)+",\""+str(dt_now)+"\","+str(elpased_time)+"\n"
        
        
        rules["default"]["name_of_datasets"]=str(rules["default"]["name_of_datasets"])[:-1]
        with open(".//app//source//configfile.ini", 'w') as configfile:
            rules.write(configfile)

        log_calls("run_generation",main_start_time,'new_run')
        with open(".//app//experiments//graph//data_generation_times.csv", 'a+',newline="") as f_out:
            f_out.write(stat_data)
            f_out.close()
    except Exception as e:
        log_error("run_generation",e)
def main():
    run_generation(number_of_datasets=3,output_path=".//app//source//generator//output//")

if __name__=="__main__":
    main()
