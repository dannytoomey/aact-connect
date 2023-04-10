## AACT Connect

This repository contains a command line executable python file (aact-connect.py) and its dependencies.

## UNIX Setup

1. Create a free AACT account [here](https://aact.ctti-clinicaltrials.org/users/sign_up) 

2. Clone or download this repository ([tutorial here](https://www.tutorialspoint.com/how-to-clone-a-github-repository))

3. Navigate into repository with `cd aact-python` 

4. Install dependencies with `python3 -m pip install -r requirements.txt`

5. Activate the virtual environment with `source env/bin/activate`

## Usage

There are several flags you can use to run different commands

To perform the main search, enter
`python aact-connect.py -u [your acct username] -p [your aact password] -s`

_Optional: To avoid having to re-enter your credentials every time you run the command, create a text file in the `private` directory called `myconfig.txt`. Then type your username on the top line and your password on the second line, so that the text file reads as:_
```
[username]
[password]
```

The results of the search will be in the `queries` directory with the file name `query_[number of results]_results.csv`

To add additional data of interest to the search results, enter
`python aact-connect.py -u [your acct username] -p [your aact password] -s -a`

The results of the data additions will be in the `additional_data` directory with the file name `query_[number of results]_additional_data.csv`

_Note: This operation can take a few minutes. A progress bar with an estimate time to completion is provided_

To search for adverse event data for a given clinical trials, enter
`python aact-connect.py -u [your acct username] -p [your aact password] -l [NCT id]`

The results of the AE look up will be in the `AE_lookup` directory with the file name `AE_lookup_[NCT id].csv`
