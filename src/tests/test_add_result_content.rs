use crate::add_results::add_results::add_results;
use crate::add_results::add_struct_to_polars::add_struct_to_polars;
use crate::get_results::get_results::get_results;
use crate::get_results::result_struct_to_polars::result_struct_to_polars;
use polars::prelude::*;
use std::env::var;
use std::error::Error;

#[tokio::test]
async fn number_of_nsae_subjects() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let number_of_nsae_subjects: Vec<Option<i32>> = row
        .column("number_of_nsae_subjects")?
        .i32()?
        .into_iter()
        .collect();
    assert_eq!(1344, number_of_nsae_subjects[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn minimum_age_num() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let minimum_age_num: Vec<Option<i32>> =
        row.column("minimum_age_num")?.i32()?.into_iter().collect();
    assert_eq!(6, minimum_age_num[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn maximum_age_num() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let maximum_age_num: Vec<Option<i32>> =
        row.column("maximum_age_num")?.i32()?.into_iter().collect();
    assert!(maximum_age_num[0].is_none());
    Ok(())
}
#[tokio::test]
async fn design_groups() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let design_groups: Vec<Option<&str>> =
        row.column("design_groups")?.utf8()?.into_iter().collect();
    assert_eq!(
        "Participants were randomized to receive AZLI for up to 24 weeks and may have continued to receive AZLI during the open-label phase for up to an additional 24 weeks.; Participants were randomized to receive placebo to match AZLI for up to 24 weeks and may have switched to AZLI during the open-label phase for up to 24 weeks.",
        design_groups[0].unwrap()
    );
    Ok(())
}
#[tokio::test]
async fn interventions() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let interventions: Vec<Option<&str>> =
        row.column("interventions")?.utf8()?.into_iter().collect();
    assert_eq!(
        "Aztreonam for inhalation solution (AZLI; 75 mg aztreonam/52.5 mg lysine monohydrate) was administered three times a day, with at least 4 hours between doses, using the investigational nebulizer.; Placebo to match AZLI (lactose and sodium chloride) was administered three times a day, with at least 4 hours between doses, using the investigational nebulizer.",
        interventions[0].unwrap()
    );
    Ok(())
}
#[tokio::test]
async fn p_value() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let p_value: Vec<Option<&str>> = row.column("p_value")?.utf8()?.into_iter().collect();
    assert_eq!(
        "0.663; 0.4158; 0.939; 0.711; 0.762; 0.553; 0.17; 0.528; 0.132; 0.531; 0.232; 0.103; 0.646; 0.284",
        p_value[0].unwrap()
    );
    Ok(())
}
#[tokio::test]
async fn ci_percent() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let ci_percent: Vec<Option<&str>> = row.column("ci_percent")?.utf8()?.into_iter().collect();
    assert_eq!(
        "95.0; 95.0; 95.0; 95.0; 95.0; 95.0; 95.0; 95.0; 95.0; 95.0",
        ci_percent[0].unwrap()
    );
    Ok(())
}
#[tokio::test]
async fn pmid() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let pmid: Vec<Option<&str>> = row.column("pmid")?.utf8()?.into_iter().collect();
    assert!(pmid[0].is_none());
    Ok(())
}
#[tokio::test]
async fn citation() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let citation: Vec<Option<&str>> = row.column("citation")?.utf8()?.into_iter().collect();
    assert!(citation[0].is_none());
    Ok(())
}
#[tokio::test]
async fn recruitment_details() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let recruitment_details: Vec<Option<&str>> = row
        .column("recruitment_details")?
        .utf8()?
        .into_iter()
        .collect();
    assert_eq!(
        "Participants were enrolled at 34 sites in the United States and 1 site in Canada. The first participant was screened on 22 February 2010. The last participant observation was on 28 December 2010.",
        recruitment_details[0].unwrap()
    );
    Ok(())
}
#[tokio::test]
async fn ae_count() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let ae_count: Vec<Option<i64>> = row.column("ae_count")?.i64()?.into_iter().collect();
    assert_eq!(175, ae_count[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn sae_count() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let sae_count: Vec<Option<i64>> = row.column("sae_count")?.i64()?.into_iter().collect();
    assert_eq!(81, sae_count[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn mortality_count() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let mortality_count: Vec<Option<i64>> =
        row.column("mortality_count")?.i64()?.into_iter().collect();
    assert_eq!(0, mortality_count[0].unwrap());
    Ok(())
}
#[tokio::test]
async fn num_aes_described() -> Result<(), Box<dyn Error>> {
    let get_data = get_data().await?;
    let row = get_data.clone().lazy().collect().unwrap();
    let num_aes_described: Vec<Option<i64>> = row
        .column("num_aes_described")?
        .i64()?
        .into_iter()
        .collect();
    assert_eq!(102, num_aes_described[0].unwrap());
    Ok(())
}

async fn get_data() -> Result<DataFrame, Box<dyn Error>> {
    let un = var("USERNAME").unwrap_or_else(|_| String::new());
    let pw = var("PASSW").unwrap_or_else(|_| String::new());
    let test_results = get_results(un.as_str(), pw.as_str()).await?;
    let polars_struct_test = result_struct_to_polars(test_results, true);
    let included_results = polars_struct_test
        .unwrap()
        .lazy()
        .filter(col("nct_id").eq(lit("NCT01059565")))
        .collect()
        .unwrap();
    let add_test = add_results(un.as_str(), pw.as_str(), included_results, 8).await?;
    let add_test_polars = add_struct_to_polars(add_test);
    Ok(add_test_polars.unwrap())
}
