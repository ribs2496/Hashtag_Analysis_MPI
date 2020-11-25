#!/bin/bash
#SBATCH --time=0-30:0:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=physical

# Load required modules
module load Python/3.5.2-intel-2017.u2

#time mpiexec -n 8 python Assignment1.py -i bigTwitter.json
time mpiexec -n 1 python Assignment1.py - bigTwitter.json
#time mpiexec -n 8 python Assignment1.py -i bigTwitter.json
