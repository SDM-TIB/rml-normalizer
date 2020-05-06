from concurrent.futures import ThreadPoolExecutor
import os
from configparser import ConfigParser, ExtendedInterpolation
import time
from datetime import datetime
import shutil

global stat_data

def install_tools():
    if os.path.exists("rmlmapper")==True:
        if os.path.exists(".\\rmlmapper\\target_backup\\target\\")==True:
            shutil.rmtree(".\\rmlmapper\\target_backup\\target\\")
        shutil.copytree(".\\rmlmapper\\target\\", ".\\rmlmapper\\target_backup\\target\\")
        shutil.rmtree(".\\rmlmapper\\target\\")
        os.system("git -C .\\rmlmapper pull origin master")
        pom=[]
        with open(".\\rmlmapper\\pom.xml", 'r') as f_read:
            pom=f_read.readlines()
            pom_bkp=pom[:]
            pom_finalname=list(filter(lambda x: "<finalName>" in x, pom))
            pom_line=pom.index(pom_finalname[0])
            pom_finalname_value=pom[pom_line].strip()
            pom[pom_line]=pom[pom_line].replace(pom_finalname_value,"<finalName>rmlmapper</finalName>")
        f_write=open(".\\rmlmapper\\pom.xml", 'w') 
        f_write.writelines(pom)
        f_write.close()
        os.system("mvn clean install -DskipTests -f .\\rmlmapper")
        f_write=open(".\\rmlmapper\\pom.xml", 'w') 
        f_write.writelines(pom_bkp)
        f_write.close()
        #os.system("mvn clean install -DskipTests -f C:\\Users\TorabinejadM\Desktop\Thesis\implementation\\normalization-experiments\\rmlmapper")
    else:
        os.system("git clone https://github.com/RMLio/rmlmapper-java.git rmlmapper")
        os.system("mvn clean install -DskipTests -f .\\rmlmapper")
    
    if os.path.exists("rocketrml")==True:
        os.system("git -C .\\rocketrml pull origin master")
    else:
        os.system("git clone https://github.com/semantifyit/RocketRML.git rocketrml")
    
    if os.path.exists("sdm-rdfizer")==True:
        os.system("git -C .\\sdm-rdfizer pull origin master")
    else:
        os.system("git clone https://github.com/SDM-TIB/SDM-RDFizer.git sdm-rdfizer")

def run_experiments():
    global stat_data
    stat_data=""
    #print(__name__)
    os.system("echo ***************Start***************************")
    #default_main_dir="C:\\Users\TorabinejadM\Desktop\Thesis\implementation\\sdm-tib-github\\rml-normalizer\\experiments"
    default_main_dir="."
    default_map_folder="\\mappings\\"
    default_output_folder="\\graph\\"
    default_config_folder="\\rdfizer\\"
    default_stats_header="NAME,RDFIZER,STARTED_AT,EXECUTION_TIME"
    default_stats_filename="all_stats.csv"
    #used only for SDM-rdfizer
    default_config_filename="configfile.ini"
    #default_main_dir_var="${default:main_directory}"
    
    mapping_name="mapping"
    rdfizers=["rocketrml","sdm-rdfizer","rmlmapper"]
    
    #used only for SDM-rdfizer
    config_file=default_main_dir+"\\sdm-rdfizer\\rdfizer\\"+default_config_filename
    
    dt_format='%d.%m.%Y %H:%M:%S,%f'
    
    #Due to complexities for cloning and installing rocketrml, the development of this function is already stopped. It can be dveloped further at a later time.
    #install_tools()
    
    try:
        with open(default_main_dir+default_output_folder+default_stats_filename, 'r') as f_in:
            default_stats_header=f_in.readline()
    except FileNotFoundError as e:
        with open(default_main_dir+default_output_folder+default_stats_filename, 'a+',newline="") as f_out:
            f_out.write(default_stats_header+"\n")
    
    entries = os.listdir(default_main_dir+default_map_folder)
    print(entries)
    for entry in entries:
        if entry.endswith('.ttl'):
            mapping_name=entry
            mapping_file=default_main_dir+default_map_folder+mapping_name
            #mapping_file=default_main_dir+default_map_folder+mapping_name+".ttl"
            
            for i in rdfizers:
                os.system("echo *************** "+i+" is rdfizing dataset "+mapping_name+" ***************************")
                #output_name=str(os.path.splitext(entry)[0])+"-"+rdfizers[i]
                output_name=str(os.path.splitext(entry)[0])+"-"+i
                output_path=default_main_dir+default_output_folder
                output_file=output_path+output_name+".nt"
                
                if i=="sdm-rdfizer":
                    config = ConfigParser(interpolation=ExtendedInterpolation())
                    config.read(config_file)
                    #config.set("default", "main_directory",default_main_dir)
                    config.set("datasets", "output_folder",output_path)
                    config.set("datasets", "number_of_datasets","1")
                    config.set("datasets", "name",mapping_name)
                    config.set("dataset1", "name",output_name)
                    config.set("dataset1", "mapping",mapping_file)
                    with open(config_file, 'w') as configfile:
                        config.write(configfile)
                    v_call="python .\\sdm-rdfizer\\rdfizer\\run_rdfizer.py "+ config_file
                elif i=="rmlmapper":
                    v_arguments="-m "+mapping_file+" -o "+output_file + " -d"
                    v_call="java -jar .\\rmlmapper\\target\\rmlmapper.jar "+v_arguments
                elif i=="rocketrml":
                    v_arguments=mapping_file+" "+output_file
                    v_call="node .\\rocket-rml\\index.js "+v_arguments
            
                dt_now = datetime.now().strftime(dt_format)
                start_time=time.time()
                print("started at:",dt_now)
                os.system(v_call)
                v_elpased_time=time.time()-start_time
                print("execuion time: {} seconds".format(v_elpased_time))
                #stat_data=stat_data+mapping_name+","+rdfizers[i]+",\""+str(dt_now)+"\","+str(v_elpased_time)+"\n"
                stat_data=stat_data+mapping_name+","+i+",\""+str(dt_now)+"\","+str(v_elpased_time)+"\n"
                #print(stat_data)
                
                #To remove extra files generated by SDM-rdfizer
                if i=="sdm-rdfizer":
                    os.remove(default_main_dir+default_output_folder+"stats.csv")
                    os.remove(default_main_dir+default_output_folder+mapping_name+"_datasets_stats.csv")
        else:
            pass
    
    with open(default_main_dir+default_output_folder+default_stats_filename, 'a+',newline="") as f_out:
        f_out.write(stat_data)
        f_out.close()
        
def main():
    #with ThreadPoolExecutor(max_workers=10) as executor:
    print("---------------------- Experiments started ... ----------------------")
    #executor.submit(run_experiments)
    run_experiments()
    print("---------------------- Experiments finished! ----------------------")


if __name__=="__main__":
    main()