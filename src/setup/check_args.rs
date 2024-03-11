use crate::structs::args::Args;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub fn check_args(mut args: Args) -> Result<Args, &'static str> {
    if Path::new("./private/myconfig.txt").exists() == false {
        if args.username == None || args.password == None {
            return Err("Please specify your crednentials with the arguments:
    -u      - your AACT username
    -p      - your AACT password

You can also create a directory named `private` and create a file named `myconfig.txt` in it, so that `./private/myconfig.txt` read as:
    <your username>
    <your password>
        ");
        }
    }
    if Path::new("./private/myconfig.txt").exists() {
        let filename = "./private/myconfig.txt";
        let file = File::open(filename).unwrap();
        let reader = BufReader::new(file);
        for (index, line) in reader.lines().enumerate() {
            match index {
                0 => args.username = Some(line.unwrap().clone()),
                1 => args.password = Some(line.unwrap().clone()),
                _ => {}
            }
        }
    }
    if args.search == false && args.current_frame == false && args.existing_frame == None {
        return Err("Please specify a command. Available options are:
    -s     - perform the query specified in ./query_text/
    -cef   - compare the orginal 4/15/23 results to a specified dataset in ./additional_data/
    ");
    } else {
        Ok(args)
    }
}
