use crate::get_results::get_results::get_results;
use crate::get_results::result_struct_to_polars::result_struct_to_polars;
use chrono::{Duration, NaiveDate};
use polars::export::num::ToPrimitive;
use polars::lazy::frame::IntoLazy;
use polars::prelude::*;
use std::env::var;
use std::error::Error;

#[tokio::test]
async fn nct_id() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let nct_id: Vec<Option<&str>> = row.column("nct_id")?.utf8()?.into_iter().collect();
    assert_eq!("NCT03135899", nct_id[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn nlm_download_date_description() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let nlm_download_date_description: Vec<Option<&str>> = row
        .column("nlm_download_date_description")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(nlm_download_date_description[0].is_none());
    Ok(())
}
#[tokio::test]
async fn study_first_submitted_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "study_first_submitted_date").unwrap(),
        NaiveDate::parse_from_str("4/26/2017", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn results_first_submitted_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "results_first_submitted_date").unwrap(),
        NaiveDate::parse_from_str("11/7/2019", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn disposition_first_submitted_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let disposition_first_submitted_date: Vec<Option<i32>> = row
        .column("disposition_first_submitted_date")?
        .date()?
        .into_iter()
        .collect();
    assert!(disposition_first_submitted_date[0].is_none());
    Ok(())
}
#[tokio::test]
async fn last_update_submitted_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "last_update_submitted_date").unwrap(),
        NaiveDate::parse_from_str("11/7/2019", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn study_first_submitted_qc_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "study_first_submitted_qc_date").unwrap(),
        NaiveDate::parse_from_str("4/26/2017", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn study_first_posted_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "study_first_posted_date").unwrap(),
        NaiveDate::parse_from_str("5/2/2017", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn study_first_posted_date_type() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let study_first_posted_date_type: Vec<Option<&str>> = row
        .column("study_first_posted_date_type")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!("ACTUAL", study_first_posted_date_type[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn results_first_submitted_qc_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "results_first_submitted_qc_date").unwrap(),
        NaiveDate::parse_from_str("11/7/2019", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn results_first_posted_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "results_first_posted_date").unwrap(),
        NaiveDate::parse_from_str("11/27/2019", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn results_first_posted_date_type() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let results_first_posted_date_type: Vec<Option<&str>> = row
        .column("results_first_posted_date_type")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!("ACTUAL", results_first_posted_date_type[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn disposition_first_submitted_qc_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let disposition_first_submitted_qc_date: Vec<Option<i32>> = row
        .column("disposition_first_submitted_qc_date")?
        .date()?
        .into_iter()
        .collect();
    assert!(disposition_first_submitted_qc_date[0].is_none());
    Ok(())
}
#[tokio::test]
async fn disposition_first_posted_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let disposition_first_posted_date: Vec<Option<i32>> = row
        .column("disposition_first_posted_date")?
        .date()?
        .into_iter()
        .collect();
    assert!(disposition_first_posted_date[0].is_none());
    Ok(())
}
#[tokio::test]
async fn disposition_first_posted_date_type() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let disposition_first_posted_date_type: Vec<Option<&str>> = row
        .column("disposition_first_posted_date_type")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(disposition_first_posted_date_type[0].is_none());
    Ok(())
}
#[tokio::test]
async fn last_update_submitted_qc_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "last_update_submitted_qc_date").unwrap(),
        NaiveDate::parse_from_str("11/7/2019", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn last_update_posted_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "last_update_posted_date").unwrap(),
        NaiveDate::parse_from_str("11/27/2019", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn last_update_posted_date_type() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let last_update_posted_date_type: Vec<Option<&str>> = row
        .column("last_update_posted_date_type")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!("ACTUAL", last_update_posted_date_type[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn start_month_year() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let start_month_year: Vec<Option<&str>> = row
        .column("start_month_year")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!("2017-05-18", start_month_year[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn start_date_type() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let start_date_type: Vec<Option<&str>> =
        row.column("start_date_type")?.utf8()?.into_iter().collect();
    assert_eq!("ACTUAL", start_date_type[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn start_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "start_date").unwrap(),
        NaiveDate::parse_from_str("5/18/2017", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn verification_month_year() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let verification_month_year: Vec<Option<&str>> = row
        .column("verification_month_year")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!("2019-11", verification_month_year[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn verification_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "verification_date").unwrap(),
        NaiveDate::parse_from_str("11/30/2019", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn completion_month_year() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let completion_month_year: Vec<Option<&str>> = row
        .column("completion_month_year")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!("2018-02-21", completion_month_year[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn completion_date_type() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let completion_date_type: Vec<Option<&str>> = row
        .column("completion_date_type")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!("ACTUAL", completion_date_type[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn completion_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "completion_date").unwrap(),
        NaiveDate::parse_from_str("2/21/2018", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn primary_completion_month_year() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let primary_completion_month_year: Vec<Option<&str>> = row
        .column("primary_completion_month_year")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!("2018-02-07", primary_completion_month_year[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn primary_completion_date_type() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let primary_completion_date_type: Vec<Option<&str>> = row
        .column("primary_completion_date_type")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!("ACTUAL", primary_completion_date_type[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn primary_completion_date() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    assert_eq!(
        get_date(row.clone(), "primary_completion_date").unwrap(),
        NaiveDate::parse_from_str("2/7/2018", "%m/%d/%Y")?
    );
    Ok(())
}
#[tokio::test]
async fn target_duration() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let target_duration: Vec<Option<&str>> =
        row.column("target_duration")?.utf8()?.into_iter().collect();
    assert!(target_duration[0].is_none());
    Ok(())
}
#[tokio::test]
async fn study_type() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let study_type: Vec<Option<&str>> = row.column("study_type")?.utf8()?.into_iter().collect();
    assert_eq!("INTERVENTIONAL", study_type[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn acronym() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let acronym: Vec<Option<&str>> = row.column("acronym")?.utf8()?.into_iter().collect();
    assert!(acronym[0].is_none());
    Ok(())
}
#[tokio::test]
async fn baseline_population() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let baseline_population: Vec<Option<&str>> = row
        .column("baseline_population")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!(
        "Treated set (TS): The TS included all patients who had been randomised and treated with at least 1 dose of trial medication. The treatment assignment was determined based on the first treatment the patients received.",
        baseline_population[0].unwrap()
    );
    Ok(())
}
#[tokio::test]
async fn brief_title() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let brief_title: Vec<Option<&str>> = row.column("brief_title")?.utf8()?.into_iter().collect();
    assert_eq!("BI 443651 Methacholine Challenge", brief_title[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn official_title() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let official_title: Vec<Option<&str>> =
        row.column("official_title")?.utf8()?.into_iter().collect();
    assert_eq!(
        "A Two Part Phase I, Multiple-dose, Single- and Double-blind, Randomised, Double-dummy, Placebo-controlled, Four-way Crossover Study to Assess Safety and Tolerability of BI 443651 Via Respimat® Versus Placebo Via Respimat® in Subjects With Mild Asthma Following Methacholine Challenge.",
        official_title[0].unwrap()
    );
    Ok(())
}
#[tokio::test]
async fn overall_status() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let overall_status: Vec<Option<&str>> =
        row.column("overall_status")?.utf8()?.into_iter().collect();
    assert_eq!("COMPLETED", overall_status[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn last_known_status() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let last_known_status: Vec<Option<&str>> = row
        .column("last_known_status")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(last_known_status[0].is_none());
    Ok(())
}
#[tokio::test]
async fn phase() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let phase: Vec<Option<&str>> = row.column("phase")?.utf8()?.into_iter().collect();
    assert_eq!("PHASE1", phase[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn enrollment() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let enrollment: Vec<Option<i32>> = row.column("enrollment")?.i32()?.into_iter().collect();
    assert_eq!(37, enrollment[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn enrollment_type() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let enrollment_type: Vec<Option<&str>> =
        row.column("enrollment_type")?.utf8()?.into_iter().collect();
    assert_eq!("ACTUAL", enrollment_type[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn source() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let source: Vec<Option<&str>> = row.column("source")?.utf8()?.into_iter().collect();
    assert_eq!("Boehringer Ingelheim", source[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn limitations_and_caveats() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let limitations_and_caveats: Vec<Option<&str>> = row
        .column("limitations_and_caveats")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(limitations_and_caveats[0].is_none());
    Ok(())
}
#[tokio::test]
async fn number_of_arms() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let number_of_arms: Vec<Option<i32>> =
        row.column("number_of_arms")?.i32()?.into_iter().collect();
    assert_eq!(2, number_of_arms[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn number_of_groups() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let number_of_groups: Vec<Option<i32>> =
        row.column("number_of_groups")?.i32()?.into_iter().collect();
    assert!(number_of_groups[0].is_none());
    Ok(())
}
#[tokio::test]
async fn why_stopped() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let why_stopped: Vec<Option<&str>> = row.column("why_stopped")?.utf8()?.into_iter().collect();
    assert!(why_stopped[0].is_none());
    Ok(())
}
#[tokio::test]
async fn has_expanded_access() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let has_expanded_access: Vec<Option<bool>> = row
        .column("has_expanded_access")?
        .bool()?
        .into_iter()
        .collect();
    assert_eq!(false, has_expanded_access[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn expanded_access_type_individual() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let expanded_access_type_individual: Vec<Option<bool>> = row
        .column("expanded_access_type_individual")?
        .bool()?
        .into_iter()
        .collect();
    assert!(expanded_access_type_individual[0].is_none());
    Ok(())
}
#[tokio::test]
async fn expanded_access_type_intermediate() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let expanded_access_type_intermediate: Vec<Option<bool>> = row
        .column("expanded_access_type_intermediate")?
        .bool()?
        .into_iter()
        .collect();
    assert!(expanded_access_type_intermediate[0].is_none());
    Ok(())
}
#[tokio::test]
async fn expanded_access_type_treatment() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let expanded_access_type_treatment: Vec<Option<bool>> = row
        .column("expanded_access_type_treatment")?
        .bool()?
        .into_iter()
        .collect();
    assert!(expanded_access_type_treatment[0].is_none());
    Ok(())
}
#[tokio::test]
async fn has_dmc() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let has_dmc: Vec<Option<bool>> = row.column("has_dmc")?.bool()?.into_iter().collect();
    assert_eq!(false, has_dmc[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn is_fda_regulated_drug() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let is_fda_regulated_drug: Vec<Option<bool>> = row
        .column("is_fda_regulated_drug")?
        .bool()?
        .into_iter()
        .collect();
    assert_eq!(false, is_fda_regulated_drug[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn is_fda_regulated_device() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let is_fda_regulated_device: Vec<Option<bool>> = row
        .column("is_fda_regulated_device")?
        .bool()?
        .into_iter()
        .collect();
    assert_eq!(false, is_fda_regulated_device[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn is_unapproved_device() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let is_unapproved_device: Vec<Option<bool>> = row
        .column("is_unapproved_device")?
        .bool()?
        .into_iter()
        .collect();
    assert!(is_unapproved_device[0].is_none());
    Ok(())
}
#[tokio::test]
async fn is_ppsd() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let is_ppsd: Vec<Option<bool>> = row.column("is_ppsd")?.bool()?.into_iter().collect();
    assert!(is_ppsd[0].is_none());
    Ok(())
}
#[tokio::test]
async fn is_us_export() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let is_us_export: Vec<Option<bool>> = row.column("is_us_export")?.bool()?.into_iter().collect();
    assert!(is_us_export[0].is_none());
    Ok(())
}
#[tokio::test]
async fn biospec_retention() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let biospec_retention: Vec<Option<&str>> = row
        .column("biospec_retention")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(biospec_retention[0].is_none());
    Ok(())
}
#[tokio::test]
async fn biospec_description() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let biospec_description: Vec<Option<&str>> = row
        .column("biospec_description")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(biospec_description[0].is_none());
    Ok(())
}
#[tokio::test]
async fn ipd_time_frame() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let ipd_time_frame: Vec<Option<&str>> =
        row.column("ipd_time_frame")?.utf8()?.into_iter().collect();
    assert!(ipd_time_frame[0].is_none());
    Ok(())
}
#[tokio::test]
async fn ipd_access_criteria() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let ipd_access_criteria: Vec<Option<&str>> = row
        .column("ipd_access_criteria")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(ipd_access_criteria[0].is_none());
    Ok(())
}
#[tokio::test]
async fn ipd_url() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let ipd_url: Vec<Option<&str>> = row.column("ipd_url")?.utf8()?.into_iter().collect();
    assert!(ipd_url[0].is_none());
    Ok(())
}
#[tokio::test]
async fn plan_to_share_ipd() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let plan_to_share_ipd: Vec<Option<&str>> = row
        .column("plan_to_share_ipd")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(plan_to_share_ipd[0].is_none());
    Ok(())
}
#[tokio::test]
async fn plan_to_share_ipd_description() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let plan_to_share_ipd_description: Vec<Option<&str>> = row
        .column("plan_to_share_ipd_description")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(plan_to_share_ipd_description[0].is_none());
    Ok(())
}
#[tokio::test]
async fn source_class() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let source_class: Vec<Option<&str>> = row.column("source_class")?.utf8()?.into_iter().collect();
    assert_eq!("INDUSTRY", source_class[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn delayed_posting() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let delayed_posting: Vec<Option<bool>> =
        row.column("delayed_posting")?.bool()?.into_iter().collect();
    assert!(delayed_posting[0].is_none());
    Ok(())
}
#[tokio::test]
async fn expanded_access_nctid() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let expanded_access_nctid: Vec<Option<&str>> = row
        .column("expanded_access_nctid")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(expanded_access_nctid[0].is_none());
    Ok(())
}
#[tokio::test]
async fn expanded_access_status_for_nctid() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let expanded_access_status_for_nctid: Vec<Option<&str>> = row
        .column("expanded_access_status_for_nctid")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(expanded_access_status_for_nctid[0].is_none());
    Ok(())
}
#[tokio::test]
async fn fdaaa801_violation() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let fdaaa801_violation: Vec<Option<bool>> = row
        .column("fdaaa801_violation")?
        .bool()?
        .into_iter()
        .collect();
    assert!(fdaaa801_violation[0].is_none());
    Ok(())
}
#[tokio::test]
async fn baseline_type_units_analyzed() -> Result<(), Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let row = polars_struct_test
        .unwrap()
        .clone()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT03135899")))
        .collect()
        .unwrap();
    let baseline_type_units_analyzed: Vec<Option<&str>> = row
        .column("baseline_type_units_analyzed")?
        .utf8()?
        .into_iter()
        .collect();
    assert!(baseline_type_units_analyzed[0].is_none());
    Ok(())
}

fn get_date(row: DataFrame, col: &str) -> Result<NaiveDate, Box<dyn Error>> {
    let date_i32: Vec<Option<i32>> = row.column(col)?.date()?.into_iter().collect();
    let date_chrono = NaiveDate::from_ymd_opt(1970, 1, 1)
        .expect("DATE")
        .checked_add_signed(Duration::days(date_i32[0].unwrap().to_i64().unwrap()));
    Ok(date_chrono.unwrap())
}
