use crate::add_results::add_results::add_results;
use crate::add_results::add_struct_to_polars::add_struct_to_polars;
use crate::get_results::get_results::get_results;
use crate::get_results::result_struct_to_polars::result_struct_to_polars;
use crate::tests::utils::print_type_of;
use std::env::var;
use std::error::Error;

#[tokio::test]
async fn test_results() -> Result<(), Box<dyn Error>> {
    let username = var("USERNAME").unwrap_or_else(|_| String::new());
    let password = var("PASSWORD").unwrap_or_else(|_| String::new());
    println!("{:?}", username);
    let username = username.as_str();
    let password = password.as_str();
    let test_results = get_results(username, password).await?;
    assert_eq!(
        "alloc::vec::Vec<aact_connect::structs::connect_results::ConnectResults>",
        print_type_of(&test_results)
    );
    let polars_struct_test = result_struct_to_polars(test_results);
    assert_eq!(
        "polars_core::frame::DataFrame",
        print_type_of(polars_struct_test.as_ref().unwrap())
    );
    let add_test = add_results(username, password, polars_struct_test.unwrap(), 64).await?;
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
