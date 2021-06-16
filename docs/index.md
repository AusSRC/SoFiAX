## SoFiAX

**So**urce **Fi**nding **A**pplication e**X**ectuor

An [AusSRC](https://aussrc.org/) project.

A Python wrapper for executing [SoFiA](https://github.com/SoFiA-Admin/SoFiA-2) and automatically merging, resolving and inserting output products to a selected PostgreSQL database. Sources that cannot be resolved automatically will be flagged for manual inspection, which can be done by users through SoFiAX's web portal ([SoFiAX_services](https://github.com/AusSRC/SoFiAX_services)). 

Written by AusSRC software research engineers for WALLABY project scientists who are running SoFiA.

## Usage

### Installation

The installation of SoFiAX requires `python3.8` or greater. You can check the installed version on your machine with `python --version`. SoFiAX can be run with and without an installation of SoFiA. This is specified in the configuration file.

#### With `sofia`

SoFiAX is intended to be a wrapper around SoFiA. To use it in this way you will need SoFiA installed on your machine. The instructions below show how both SoFiA and SoFiAX can be installed.

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

#### Without `sofia`

SoFiAX can be run without a local SoFiA installation to write data outputted from a SoFiA execution to a database of your choosing. 

```
git clone https://github.com/ICRAR/SoFiAX
cd SoFiAX
python3 -m venv env
source env/bin/activate
python setup.py install
```

### Execution

There are scripts are available for generating this configuration file automatically from the image cube of interest. See: [https://github.com/SoFiA-Admin/s2p_setup](https://github.com/SoFiA-Admin/s2p_setup)

#### Locally

```
sofiax -c config.ini -p sofia.par
```

#### Docker

We have an [official Docker image](https://hub.docker.com/r/aussrc/sofiax) for the AusSRC SoFiAX application. 

```
docker pull aussrc/sofiax
docker run sofiax -c config.ini -p sofia.par
```

#### Slurm

To run SoFiAX on a Slurm cluster you will need the `sofia.par` and `config.ini` files. We recommend you utilise the [`s2p_setup`](https://github.com/SoFiA-Admin/s2p_setup) code for generating the approriate configuration files for the data cube of interest.

This `s2p_setup` repository generates a `run_sofiax.sh` file that can be run from the Slurm head node.

### Configuration

The SoFiAX configuration is set in the `config.ini` file. The file contains all of the SoFiAX-specific information. Other information required to run `sofiax` is contained in the `sofia.par` file.

#### config.ini

##### Template

```
[SoFiAX]
db_hostname=
db_name=
db_username=
db_password=

sofia_execute=0
sofia_path=/<path>/sofia
sofia_processes=1

run_name = Test
spatial_extent = 5, 5
spectral_extent = 5, 5
flux = 5
uncertainty_sigma = 5
```

##### Descriptions

| Parameter | Description  |
--- | --- |
| `db_hostname` | Host address for the database that you wish to write the SoFiA output products to. |
| `db_name` | Name of the database. |
| `db_username` | Username for a user to access to specified database. |
| `db_password` | Password for a user to access to specified database. |
| `sofia_execute` | Whether or not to execute `sofia`. `1` will launch `sofia` as part of `sofiax`, or `0` to run `sofiax` on existing output products |
| `sofia_path` | Only required if `sofiax_execute=1`. Path to the `sofia` executable in the execution environment. |
| `sofia_processes` | Only required if `sofia_execute=1`. Number of processes across which to execute `sofia`. |
| `run_name` | Name of the run, used to identify the products in the database. |
| `spatial_extent ` | SoFiA attribute |
| `spectral_extent` | SoFiA attribute |
| `flux` | SoFiA attribute |
| `uncertainty_sigma` | SoFiA attribute |

#### sofia.par

The `sofia.par` file allows the user to customise the run of `sofia`. This file is required even if `sofia_execute=0` as the location of output files is contained in this parameter file. This codebase is maintained by the WALLABY science team, and the relavant links are:

* [Repository](https://github.com/SoFiA-Admin/SoFiA-2)
* [Official wiki](https://github.com/SoFiA-Admin/SoFiA-2/wiki)
* [Description of parameters](https://github.com/SoFiA-Admin/SoFiA-2/wiki/SoFiA-2-Control-Parameters)

### Test

To verify the installation of SoFiAX is successful you can download and run SoFiAX on the test case. Download the files:

* [SoFiAX test case]() (123.9 MB)

First step is to extract the contents of the file:

```
tar -xzvf sofia_test_case.tar.gz
```

This should create a folder called `sofiax_test_case`. Now you will need a PostgreSQL database with the correct schema installed, which can be done via our initalisation script in this [SoFiAX_services repository](https://github.com/AusSRC/SoFiAX_services/tree/main/db). You will also need to update the SoFiAX `config.ini` file with the credentials for your database

```
[SoFiAX]
db_hostname = <your_hostname>
db_name = <your_db_name>
db_username = <your_username>
db_password = <your_password>
```

Once you have a database with the correct schema, updated the `config.ini` file with the credentials and installed SoFiAX, you are ready to test that it runs correctly. Run the following command

```
sofiax -c sofiax_test_case/config.ini -p sofiax_test_case/sofia_058.par
```

Your command line output for a successful run of SoFiAX will be

```
$ sofiax -c test_case/config.ini -p test_case/sofia.par

2021-06-14 04:10:46,806 - root - INFO - Processing test_case/sofia.par
2021-06-14 04:10:47,008 - root - INFO - Sofia completed: test_case/sofia.par
2021-06-14 04:10:47,143 - root - INFO - No duplicates, Name: SoFiA J120120.94+615351.3
2021-06-14 04:10:47,275 - root - INFO - No duplicates, Name: SoFiA J120215.81+620729.5
2021-06-14 04:10:47,421 - root - INFO - No duplicates, Name: SoFiA J120142.94+621931.7
...
```