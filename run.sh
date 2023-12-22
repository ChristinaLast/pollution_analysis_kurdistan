#!/bin/bash

# Slurm sbatch options

#SBATCH -o main_getZ.sh.log-%j
#SBATCH --gres=gpu:volta:1
#SBATCH -c 20

# Loading the required module
source /etc/profile

module load anaconda/2023a

# Run the script
python pollution_analysis/main.py cluster_flaring_data