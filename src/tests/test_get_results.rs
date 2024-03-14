use crate::get_results::get_results::get_results;
use crate::get_results::result_struct_to_polars::result_struct_to_polars;
use crate::tests::utils::get_type_of;
use std::env::var;
use std::error::Error;

#[tokio::test]
async fn test_get_results() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    assert_eq!(
        "alloc::vec::Vec<aact_connect::structs::connect_results::ConnectResults>",
        get_type_of(&test_results)
    );
    let polars_struct_test = result_struct_to_polars(test_results, true);
    assert_eq!(
        "polars_core::frame::DataFrame",
        get_type_of(polars_struct_test.as_ref().unwrap())
    );
    Ok(())
}
