from data_generator.source.generator.generate_data import run_generation
from experiments.run_experiments import run_experiments
from source.normalization.normalize import normalize
import sys
import os

#os.system("echo ====================== data generation started ======================")
run_generation(number_of_datasets=int(3),output_path=str(".//app//source//data//"))
#os.system("echo ====================== data generation finished ======================")

#os.system("echo ====================== normalization started ======================")
normalize(config_path=".//app//source//configfile.ini",csv_output_path="csv//",mapping_output_path=".//app//experiments//mappings//")
#os.system("echo ====================== normalization finished ======================")

#os.system("echo ====================== experiments started ======================")
run_experiments()
#os.system("echo ====================== experiments finished ======================")
