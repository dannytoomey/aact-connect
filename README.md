## AACT Connect

This repository contains a command line executable python file (aact-connect.py) along with with an environment directory (/env/) containing the dependencies of the files.

## Setup

1. Create a free AACT account [here](https://aact.ctti-clinicaltrials.org/users/sign_up) 

2. Clone or download this repository ([tutorial here](https://www.tutorialspoint.com/how-to-clone-a-github-repository))

3. Navigate into repository with `cd aact-connect` 

4. Activate the virtual environment with `source env/bin/activate`

## Usage

There are several flags you can use to run different commands

To perform the main search, enter
`python aact-connect.py -u [your acct username] -p [your aact password] -s`

The results of the search will be in the main directory with the file name `query_results.csv`

To add additional data of interest to the search results, enter
`python aact-connect.py -u [your acct username] -p [your aact password] -s -a`

The results of the data additions will be in the main directory with the file name `results_additional_data.csv`

_Note: This operation can take a few minutes. A progress bar with an estimate to completion is provided_

To search for adverse event data for a given clinical trials, enter
`python aact-connect.py -u [your acct username] -p [your aact password] -l [NCT id]`

The results of the AE look up will be in the main directory with the file name `AE_lookup_[NCT id].csv`
