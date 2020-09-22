from generator.generate_data import run_generation
import sys
run_generation(number_of_datasets=int(sys.argv[1]),output_path=str(sys.argv[2]))
