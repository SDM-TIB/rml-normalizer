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

global fd_dict

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

def set_fd(last_range=5):    
    global fd_dict
    fd_dict = {
        "level1": {
            "is_parent": True,
            "number_of_distinct_vals": last_range,
            "total_vals": 0,
            "dups": 2,
            "number_of_dup_values": last_range
        },
        "level2": {
            "is_parent": True,
            "number_of_distinct_vals": 2,
            "total_vals": 0,
            "dups": 0,
            "number_of_dup_values": 2
        },
        "level3": {
            "is_parent": False,
            "number_of_distinct_vals": 2,
            "total_vals": 0,
            "dups": 3,
            "number_of_dup_values": 2
        },
        "level4": {
            "is_parent": False,
            "number_of_distinct_vals": 2,
            "total_vals": 0,
            "dups": 4,
            "number_of_dup_values": 2
        },
        "level5": {
            "is_parent": False,
            "number_of_distinct_vals": 2,
            "total_vals": 0,
            "dups": 3,
            "number_of_dup_values": 2
        },
        "level6": {
            "is_parent": False,
            "number_of_distinct_vals": 2,
            "total_vals": 0,
            "dups": 2,
            "number_of_dup_values": 2
        },
        "level7": {
            "is_parent": True,
            "number_of_distinct_vals": 2,
            "total_vals": 0,
            "dups": 0,
            "number_of_dup_values": 2
        },
        "level8": {
            "is_parent": False,
            "number_of_distinct_vals": 15625,
            "total_vals": 0,
            "dups": 0,
            "number_of_dup_values": 0
        }
    }
    return fd_dict


def update_input(current_level=1):
    global fd_dict
    v_add_up=1
    v_distinct_values=2
    is_parent=fd_dict["level"+str(current_level)]["is_parent"]
    total_vals=fd_dict["level"+str(current_level)]["total_vals"]
    number_of_distinct_vals=fd_dict["level"+str(current_level)]["number_of_distinct_vals"]
    
    if is_parent==False and total_vals==0 and current_level+1<=first_level:
        v_add_up=v_add_up*number_of_distinct_vals*update_input(current_level+1)
        fd_dict["level"+str(current_level)]["total_vals"]=v_add_up
    elif is_parent==False and total_vals==0 and current_level+1>first_level:
        fd_dict["level"+str(current_level)]["total_vals"]=v_distinct_values
    elif is_parent==True:
        v_add_up=number_of_distinct_vals
    return v_add_up

def set_col_headers():
    with open(output_file, 'w',newline="") as f_out:
        for i in range(1,first_level+1):
            if first_level!=i:
                f_out.write(chr(first_letter+first_level-i)+',')
            else:
                f_out.write(chr(first_letter+first_level-i)+'\n')
        f_out.close()

def generate_data(level=0,tree=Tree(),parent="",parent_index=0,out_text=[],start=1,end=3,addup=0,node_id="",last_range=5):
#def generate_data(level=0,tree=Tree(),parent="",parent_index=0,out_text="",start=1,end=3,addup=0):
    try:
        global all_out_text
        global output_file
        global v_output_file_index
        letter=first_letter+first_level-level
        if level!=0:
            num_of_dups=fd_dict["level"+str(level)]["dups"]
            is_parent=fd_dict["level"+str(level)]["is_parent"]  #it should be automatic (as input)
            if level==first_level:
                if is_parent==False:
                    num_of_distinct_vals=fd_dict["level"+str(level)]["number_of_distinct_vals"]
                    for i in range(1,num_of_distinct_vals+1):
                        v_output_file_index=v_output_file_index+1
                        #tree.create_node(chr(letter)+str(i), node_id+chr(letter)+str(i),parent)
                        ###########Using normal I/O######################
                        #all_out_text=all_out_text+out_text+chr(letter)+str(i)+"\n"
                        #with open(output_file, 'a+',newline="") as f_out:
                        #    f_out.write(out_text+chr(letter)+str(i)+"\n")
                        #    f_out.close()
                        ###########Using pandas######################
                        out_text.append(chr(letter)+str(i))
                        all_out_text.append(out_text[:])
                        del out_text[-1]
                else:
                    for i in range(start,end):
                        v_output_file_index=v_output_file_index+1
                        #tree.create_node(chr(letter)+str(i),node_id+chr(letter)+str(i),parent)
                        ###########Using normal I/O######################
                        #all_out_text=all_out_text+out_text+chr(letter)+str(i)+"\n"
                        #with open(output_file, 'a+',newline="") as f_out:
                        #    f_out.write(out_text+chr(letter)+str(i)+"\n")
                        #    f_out.close()
                        ###########Using pandas######################
                        out_text.append(chr(letter)+str(i))
                        all_out_text.append(out_text[:])
                        del out_text[-1]
            elif level==1:
                number_of_dup_values=fd_dict["level"+str(level)]["number_of_dup_values"] #it should be clacualted (according to some formula based on the total number of records)
                number_of_values=fd_dict["level"+str(level)]["number_of_distinct_vals"] #it should be clacualted (according to some formula based on the total number of records)
                for i in range(1,number_of_values+1):
                    next_num_of_distinct_vals=fd_dict["level"+str(level+1)]["number_of_distinct_vals"]
                    if i>number_of_dup_values:
                        start_point=(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)
                        end_point=(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)+1
                    else:
                        start_point=(i-1)*next_num_of_distinct_vals+1
                        end_point=(i)*next_num_of_distinct_vals+1
                    #node_id=node_id.replace(chr(letter)+str(i-1)+",","")
                    #node_id=node_id+chr(letter)+str(i)+","
                    out_text.append(chr(letter)+str(i))
                    #tree.create_node(chr(letter)+str(i),node_id,parent)
                    generate_data(level+1,tree,parent=node_id,parent_index=i,out_text=out_text,start=start_point,end=end_point,node_id=node_id)
                    del out_text[-1]
            else:
                if is_parent==False:
                    num_of_distinct_vals=fd_dict["level"+str(level)]["number_of_distinct_vals"]
                    next_num_of_distinct_vals=fd_dict["level"+str(level+1)]["number_of_distinct_vals"]
                    number_of_dup_values=fd_dict["level"+str(level)]["number_of_dup_values"]
                    for i in range(1,num_of_distinct_vals+1):
                        v_addup=addup+(parent_index-1)*fd_dict["level"+str(level)]["total_vals"]
                        not_dup_factor=parent_index-1
                        if i>number_of_dup_values:
                            start_point=v_addup+(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                            end_point=v_addup+(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)+1+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                        else:
                            start_point=v_addup+(i-1)*next_num_of_distinct_vals+1+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                            end_point=v_addup+i*next_num_of_distinct_vals+1+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                        #node_id=node_id.replace(chr(letter)+str(i-1)+",","")
                        #node_id=node_id+chr(letter)+str(i)+","
                        out_text.append(chr(letter)+str(i))
                        #tree.create_node(chr(letter)+str(i),node_id,parent)
                        generate_data(level+1,tree,parent=node_id,parent_index=i,out_text=out_text,start=start_point,end=end_point,addup=v_addup,node_id=node_id)
                        del out_text[-1]
                else:
                    for i in range(start,end):
                        num_of_distinct_vals=fd_dict["level"+str(level)]["number_of_distinct_vals"]
                        next_num_of_distinct_vals=fd_dict["level"+str(level+1)]["number_of_distinct_vals"]
                        number_of_dup_values=fd_dict["level"+str(level)]["number_of_dup_values"]
                        not_dup_factor=parent_index-1
                        if i>number_of_dup_values:
                            start_point=(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                            end_point=(number_of_dup_values)*next_num_of_distinct_vals+(i-number_of_dup_values)+1+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                        else:
                            start_point=(i-1)*next_num_of_distinct_vals+1+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                            end_point=i*next_num_of_distinct_vals+1+not_dup_factor*(number_of_dup_values-num_of_distinct_vals)
                        #node_id=node_id.replace(chr(letter)+str(i-1)+",","")
                        #node_id=node_id+chr(letter)+str(i)+","
                        out_text.append(chr(letter)+str(i))
                        #tree.create_node(chr(letter)+str(i),node_id,parent)
                        generate_data(level+1,tree,parent=node_id,parent_index=i,out_text=out_text,start=start_point,end=end_point,addup=0,node_id=node_id)
                        del out_text[-1]
        else:
            set_fd(last_range)
            for i in range(1,first_level+1):
                update_input(i)
            #tree.create_node("root", "root")
            all_out_text=[]
            generate_data(level+1,tree,parent="root")
            
            output_file=output_file+str(v_output_file_index)+"_data.csv"
            try:
                os.remove(output_file)
            except FileNotFoundError:
                pass
            set_col_headers()
            
            ###########Using pandas######################
            #text_to_save = pd.DataFrame(all_out_text, columns=['H', 'G','F','E','D','C','B','A'])
            #text_to_save.to_csv(output_file,index=False)
            ###########Using pandas######################
            
            ###########Using normal I/O######################
            with open(output_file, 'a+',newline='') as f_out:
                writer = csv.writer(f_out)
                writer.writerows(all_out_text)
                f_out.close()
            ###########Using normal I/O######################
            
            print("Done!")
            #print(tree.show())
    except Exception as e:
        print(e)

def run_generation(number_of_datasets=1,output_path=".\\output\\"):
    global output_file
    for i in range(1,number_of_datasets+1):
        dt_now = datetime.now().strftime(dt_format)
        start_time=time.time()
        print("started at:",dt_now)
        output_file=output_path
        generate_data(last_range=v_last_range*i)
        
        dt_now = datetime.now().strftime(dt_format)
        print("finished at:",dt_now)
        v_elpased_time=time.time()-start_time
        print("execuion time: {} seconds".format(v_elpased_time))
        
def main():
    run_generation(number_of_datasets=1,output_path=".\\output\\")

if __name__=="__main__":
    main()