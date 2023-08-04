## AACT Connect

This repository contains a CLI for interacting with the ClinicalTrials.gov AACT API, written in rust.

## Setup

1. Create a free AACT account [here](https://aact.ctti-clinicaltrials.org/users/sign_up) 

2. Clone or download this repository ([tutorial here](https://www.tutorialspoint.com/how-to-clone-a-github-repository))

4. Install [rust](https://www.rust-lang.org/tools/install) on your local machine. Precompiled binary files are provided as releases for 64-bit macOS 10.7+, 64-bit Windows MinGW (7+), and 64-bit Linux kernel 3.2+ (glic 2.17+). Examples will be given using `cargo run`, but binary files may be used instead for users who do not have rust installed. 

## Usage

- To perform a search using the SQL query provided in `./query_text/systematic_review_query.txt`, enter
`cargo run -- -u [your acct username] -p [your aact password] -s`

_Optional: To avoid having to re-enter your credentials every time you run the command, create a directory named `private` and place a file named `myconfig.txt` in it. Then type your username on the top line and your password on the second line, so that `private/myconfig.txt` reads as:_
```
[username]
[password]
```

The results of the search will be in the `query_results` directory with the file name `query_[number of results]_results_[search date].csv`. 

_Note: This operation is multithreaded for performance. The default number of threads used is 100. This can be adjusted by the `-t` flag followed by the number of threads you would like open on your system at once. To adjust this to an appropreiate number, refer to your system's resources._

- To generate a list of updates to included clinical trial records posted after data collection was completed on 4/15/23, enter
`cargo run -- -s -c`

This will re-generate the search and dataset and provide a list of changes made since 4/15/23. A comparison to an existing data frame can be generated with 
`cargo run -- -e additional_data/[preivously generated frame]`

## Common errors

- Error:
```
thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: Error { kind: Db, cause: Some(DbError { severity: "FATAL", parsed_severity: Some(Fatal), code: SqlState(E53300), message: "too many connections for database \"aact\"", detail: None, hint: None, position: None, where_: None, schema: None, table: None, column: None, datatype: None, constraint: None, file: Some("postinit.c"), line: Some(364), routine: Some("CheckMyDatabase") }) }'
```
- Solution:
This can be fixed by reducing the number of threads used to add results. For example: `cargo run -- -s -c -t 75`