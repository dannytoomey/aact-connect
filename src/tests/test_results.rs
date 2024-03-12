use crate::add_results::add_results::add_results;
use crate::add_results::add_struct_to_polars::add_struct_to_polars;
use crate::get_results::get_results::get_results;
use crate::get_results::result_struct_to_polars::result_struct_to_polars;
use crate::tests::utils::get_type_of;
use std::error::Error;

#[tokio::test]
async fn test_results() -> Result<(), Box<dyn Error>> {
    let test_results = get_results("dannytoomey", "aact").await?;
    assert_eq!(
        "alloc::vec::Vec<aact_connect::structs::connect_results::ConnectResults>",
        get_type_of(&test_results)
    );
    let polars_struct_test = result_struct_to_polars(test_results, true);
    assert_eq!(
        "polars_core::frame::DataFrame",
        get_type_of(polars_struct_test.as_ref().unwrap())
    );
    let add_test = add_results("dannytoomey", "aact", polars_struct_test.unwrap(), 8).await?;
    assert_eq!(
        "alloc::vec::Vec<aact_connect::structs::add_results::AddResults>",
        get_type_of(&add_test)
    );
    let add_test_polars = add_struct_to_polars(add_test);
    assert_eq!(
        "polars_core::frame::DataFrame",
        get_type_of(add_test_polars.as_ref().unwrap())
    );
    Ok(())
}
