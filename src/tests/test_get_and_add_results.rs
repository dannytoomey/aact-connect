use crate::add_results::add_results::add_results;
use crate::add_results::add_struct_to_polars::add_struct_to_polars;
use crate::get_results::get_results::get_results;
use crate::get_results::result_struct_to_polars::result_struct_to_polars;
use crate::tests::utils::print_type_of;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[tokio::test]
async fn test_get_results_and_struct() -> Result<(), Box<dyn Error>> {
    let mut username = String::new();
    let mut password = String::new();
    let filename = "./private/myconfig.txt";
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);
    for (index, line) in reader.lines().enumerate() {
        match index {
            0 => username = Some(line.unwrap().clone()).unwrap(),
            1 => password = Some(line.unwrap().clone()).unwrap(),
            _ => {}
        }
    }
    let test_results = get_results(username.as_str(), password.as_str()).await?;
    assert_eq!(
        "alloc::vec::Vec<aact_connect::structs::connect_results::ConnectResults>",
        print_type_of(&test_results)
    );
    let polars_struct_test = result_struct_to_polars(test_results);
    assert_eq!(
        "polars_core::frame::DataFrame",
        print_type_of(polars_struct_test.as_ref().unwrap())
    );
    let add_test = add_results(
        username.as_str(),
        password.as_str(),
        polars_struct_test.unwrap(),
        64,
    )
    .await?;
    assert_eq!(
        "alloc::vec::Vec<aact_connect::structs::add_results::AddResults>",
        print_type_of(&add_test)
    );
    let add_test_polars = add_struct_to_polars(add_test);
    assert_eq!(
        "polars_core::frame::DataFrame",
        print_type_of(add_test_polars.as_ref().unwrap())
    );
    Ok(())
}
