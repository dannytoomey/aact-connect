use crate::add_results::add_results::add_results;
use crate::add_results::add_struct_to_polars::add_struct_to_polars;
use crate::get_results::get_results::get_results;
use crate::get_results::result_struct_to_polars::result_struct_to_polars;
use crate::tests::utils::get_type_of;
use polars::prelude::*;
use std::env::var;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[tokio::test]
async fn test_add_results() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let mut included_ncts = Vec::<String>::new();
    let filename = "./comparisons/included_records.txt";
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);
    for (_, line) in reader.lines().enumerate() {
        included_ncts.push(line?);
    }
    let ids_series = Series::new("ID", included_ncts);
    let included_results = polars_struct_test
        .unwrap()
        .lazy()
        .filter(col("nct_id").is_in(lit(ids_series)))
        .collect()
        .unwrap();
    let add_test = add_results(un.as_str(), pw.as_str(), included_results, 8).await?;
    assert_eq!(
        "alloc::vec::Vec<aact_connect::structs::add_results::AddResults>",
        get_type_of(&add_test)
    );
    assert!(add_test.len() > 0);
    let add_test_polars = add_struct_to_polars(add_test);
    assert_eq!(
        "polars_core::frame::DataFrame",
        get_type_of(add_test_polars.as_ref().unwrap())
    );
    Ok(())
}
