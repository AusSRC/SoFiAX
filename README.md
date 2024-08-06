<!--
Copyright (c) 2021 AusSRC.

This file is part of SoFiAX
(see https://github.com/AusSRC/SoFiAX).

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 2.1 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.-->

# SoFiAX
An [AusSRC](https://aussrc.org/) Project.

[![Linting](https://github.com/AusSRC/SoFiAX/actions/workflows/linter.yml/badge.svg)](https://github.com/AusSRC/SoFiAX/actions/workflows/linter.yml)
[![Docker latest](https://github.com/AusSRC/SoFiAX/actions/workflows/docker-build-latest.yml/badge.svg)](https://github.com/AusSRC/SoFiAX/actions/workflows/docker-build-latest.yml)
[![Docker release](https://github.com/AusSRC/SoFiAX/actions/workflows/docker-build-release.yml/badge.svg)](https://github.com/AusSRC/SoFiAX/actions/workflows/docker-build-release.yml)

## Description

This project extends the capability of the [SoFiA-2](https://github.com/SoFiA-Admin/SoFiA-2 "SoFiA-2") source finding application for H1 sources by executing and automatically merging, resolving and inserting extracted sources into a database for further inspection and analysis. If there any source conflicts which can not be resolved, the user is invited to resolve them manually through SoFiAX's web portal.

## Installation

### Requirements:
  * Python >= 3.8
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

## Container

AusSRC maintains a SoFiAX container which holds a build of SoFiA-2.

### Docker

```
docker pull aussrc/sofiax
```

### Singularity

```
singularity pull docker://aussrc/sofiax
```


### Running a SofiAX instance:

First create a SoFiAX configuration file which contains a link to SoFiA-2 instance and database details.
  ```
  vim config.ini
  ```

  ```
  [SoFiAX]
  db_schema=wallaby
  db_hostname=wallaby.aussrc.org
  db_name=wallabydb
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
  flags = 0, 4
  ```

  * run_name [str]: unique run name.
  * sofia_execute [0..1]: If 0 then dont execute SoFiA, just parse the output if it already exists. If 1 then execute SoFiA.
  * sofa_path [str]: file path to the SoFiA-2 executable. Set to */usr/bin/sofia* if running in container.
  * sofia_processes [0..N]: number of SoFiA processes to run in parallel, driven by how many SoFiA parameter files that are given to a SoFiAX instance.
  * spatial_extent [int min (%), int max (%)]: sanity threshold for spatial extents.
  * spectral_extent [int min (%), int max (%)]: sanity threshold for spectral extents.
  * flux [int (%)]: sanity threshold for flux.
  * uncertainty_sigma [int]: multiply uncertainty by a value (5 default).
  * quality_flags [int, int, ..., int]: List of sofia detection quality flags to allow (Detections with flags other than these will not be ingested in the database, see (manual)[https://gitlab.com/SoFiA-Admin/SoFiA-2/-/wikis/documents/SoFiA-2_User_Manual.pdf])
  * perform_merge [0..1]: If 0 then don't merge the sources into the run, just do a direct import.

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

## Services

Source code for SoFiAX database, TAP service and Admin Console can be found in the [SoFiAX_services repository](https://github.com/AusSRC/SoFiAX_services "SoFiAX_services")

### Current Science Projects

* TAP/ADQL + Datalink service:
  * WALLABY: https://wallaby.aussrc.org/tap
  * DINGO: https://dingo.aussrc.org/tap

* Admin Console:
  * WALLABY: https://wallaby.aussrc.org
  * DINGO: https://dingo.aussrc.org