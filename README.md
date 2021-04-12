# SoFiAX
An [AusSRC](https://aussrc.org/) Project.

## Description

This project extends the capability of the [SoFiA-2](https://github.com/SoFiA-Admin/SoFiA-2 "SoFiA-2") source finding application for H1 sources by executing and automatically merging, resolving and inserting extracted sources into a database for further inspection and analysis. If there any source conflicts which can not be resolved, the user is invited to resolve them manually through SoFiAX's web portal. 

## Current Science Projects
   * TAP/ADQL + Datalink service: 
     * WALLABY: https://wallaby.aussrc.org/tap
     * DINGO: https://dingo.aussrc.org/tap
     
   * Admin Console: 
      * WALLABY: https://wallaby.aussrc.org
      * DINGO: https://dingo.aussrc.org

## Service Source Code
Source code for SoFiAX database, TAP service and Admin Console can be found in the [SoFiAX_services repository](https://github.com/AusSRC/SoFiAX_services "SoFiAX_services")

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
  python setup.py install
  ```

### Running a SofiAX instance:

First create a SoFiAX configuration file which contains a link to SoFiA-2 instance and database details. 
  ```
  vim config.ini
  ```
  
  ```
  [SoFiAX]
  db_hostname=wallaby.aussrc.org
  db_name=sofiadb
  db_username=
  db_password=

  sofia_execute=0
  sofia_path=/<path>/sofia
  sofia_processes=2
  
  run_name = Test
  spatial_extent = 5, 5
  spectral_extent = 5, 5
  flux = 5
  uncertainty_sigma = 5
  ```
  
  * sofia_execute [0..1]: If 0 then dont execute SoFiA, just parse the output if it already exists. If 1 then execute SoFiA.
  * sofa_path: file path to the SoFiA-2 executable.
  * sofia_processes [0..N]: number of SoFiA processes to run in parallel, driven by how many SoFiA parameter files that are given to a SoFiAX instance. 
  * run_name: unique run name.
  * spatial_extent [int min (%), int max (%)]: sanity threshold for spatial extents.
  * spectral_extent [int min (%), int max (%)]: sanity threshold for spectral extents.
  * flux [int (%)]: sanity threshold for flux.
  * uncertainty_sigma [int]: multiply uncertainty by a value (5 default).

Each run must be a given a unique name which all instances and detections will be grouped under in the database. Each run must specify the configuration file (as above) and one or more SoFiA-2 parameter file(s).
The spacial and spectral extents and flux are used as the sanity thresholds (specified as a %) which are used when a source matches another in the database. If a known source is found to be withing the threshold the source is either replaced with the existing source or ignored based on a random 'roll of the dice'. If the conflicting source is not within the specified thresholds it is marked as 'not resolved' and must tbe resolved manually within the web portal. 


Create a SoFiA-2 param file with a minimum:
* input.data
* input.region
* output.directory
* output.filename

### Run SofiAX (sofiax):

```
usage: sofiax.py [-h] -c CONF -p PARAM [PARAM ...]

Sofia standalone execution.

optional arguments:
  -h, --help            show this help message and exit
  -c CONF, --conf CONF  configuration file
  -p PARAM [PARAM ...], --param PARAM [PARAM ...]
                        sofia parameter file
```
 
### Example:

Run SoFiA with test.par  

```
sofiax -c config.ini -p test.par
```

### Slurm example:

Run multiple jobs across a slurm cluster

`sofia.sh`

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

source /<env path>/env/bin/activate && sofiax -c $1 -p $2
```

`run.sh`

```
#!/bin/bash

param=( a b c d e f g h )
for i in "${param[@]}"
do
    sbatch ./sofia.sh /<config file path>/config.ini /<sofia par dir>/sofia_$i.par
done
```


## Testing

We have written some tests for this repository. Some of the tests will require you to download the `test_case` folder containing configuration and parameter files.

To run the unit tests

```
cd tests && python -m unittest
```
