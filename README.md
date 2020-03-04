# SoFiAX
An AusSRC Project

## Description

This project extends the capability of the [SoFiA-2](https://github.com/SoFiA-Admin/SoFiA-2 "SoFiA-2") source finding application for H1 sources by executing and automatically merging, resolving and inserting extracted sources into a database for further inspection and analysis. If there any source conflicts which can not be resolved, then the user is invited to resolve them manually through SofiAX's web portal. 

## Installation

### Requirements:
  * Python >= 3.7
  * SoFiA-2

### Install code base to run SoFiAX:

  ```
  git clone https://github.com/SoFiA-Admin/SoFiA-2
  cd SoFiA-2
  ./compile.sh
  
  git clone https://github.com/ICRAR/SoFiAX
  cd SoFiAX
  python3 -m venv env
  source env/bin/activate
  pip install -r requirements.txt
  ```

### Running a SofiAX instance:

First create a SoFiAX configuration file which contains a link to SoFiA-2 instance and database details. 
  ```
  vim config.ini
  ```
  
  ```
  [Database]
  hostname=wallaby-01.aussrc.org
  name=sofiadb
  username=
  password=

  [Sofia]
  execute=0
  path=/<path>/sofia
  processes=2
  ```
  
  * execute [0..1]: If 0 then dont execute Sofia just parse the output if it already exists. If 1 then execute Sofia, deleting all the output data first.
  * path: file path to the SoFiA-2 executable.
  * processes [0..N]: Number of Sofia processes to run in parallel, driven by how many Sofia paramaeter files that are given to a SofiAX instance. 
  
  
Create a SoFiA-2 param file with a minimum:
* input.data
* input.region
* output.directory
* output.filename

### Run SofiAX (standalone.py):

 ```
usage: standalone.py [-h] --name NAME --spatial_extent SPATIAL [SPATIAL ...]
                     --spectral_extent SPECTRAL [SPECTRAL ...] --flux FLUX -c
                     CONF -p PARAM [PARAM ...]

Sofia standalone execution.

optional arguments:
  -h, --help            show this help message and exit
  --name NAME           unique run name
  --spatial_extent SPATIAL [SPATIAL ...]
                        sanity threshold for spatial extents (min % max %)
  --spectral_extent SPECTRAL [SPECTRAL ...]
                        sanity threshold for spectral extents (min % max %)
  --flux FLUX           sanity threshold for flux (%)
  -c CONF, --conf CONF  configuration file
  -p PARAM [PARAM ...], --param PARAM [PARAM ...]
                        sofia parameter file
  ```
  
Each run must be a given a unique name (NAME) which all instances and detections will be grouped under in the web portal. Each run must as specify the configuration file (as above) and one or more SofiA-2 paramater file(s).
The spacial and spectral extents and flux are used as the sanity thresholds (specified as a %) which are used when a source matches another in the database. If a known source is found to be withing the threshold the source is either replaced with the existing source or ignored based on a random 'roll of the dice'. If the conflicting source is not within the specified thresholds it is marked as 'not resolved' and must tbe resolved manually within the web portal. 

### Example:

Run SoFiA with test.par within a 5% santity extent of for all sources.  
```
python standalone.py --name test --spatial_extent 5 5 --spectral_extent 5 5 --flux 5 -c config.ini -p test.par
```

### Slurm example:

Run multiple jobs across a slurm cluster

sofia.sh
```
#!/bin/bash

#SBATCH --job-name=sofia_test
#SBATCH --output=/<log path>/sofia_test_%j.out
#SBATCH --error=/<log path>/sofia_test_%j.err
#SBATCH -N 1 # 2 nodes
#SBATCH -n 1 # 8 tasks
#SBATCH -c 2
#SBATCH --mem=100G

module load openssl/default
module load python/3.7.4

source /<env path>/env/bin/activate && python /<sofiax path>/SoFiAX/sofiax/sofiax/standalone.py --name $1 --spatial_extent $2 --spectral_extent $3 --flux $4 -c $5 -p $6
```

run.sh
```
#!/bin/bash

param=( a b c d e f g h )
for i in "${param[@]}"
do
    sbatch ./sofia.sh sofia_test "5 5" "5 5" 5 /<config file path>/config.ini /<sofia par dir>/sofia_$i.par
done
```
