use crate::structs::add_results::AddResults;
use polars::df;
use polars::prelude::*;
use std::error::Error;

pub fn add_struct_to_polars(vec: Vec<AddResults>) -> Result<DataFrame, Box<dyn Error>> {
    let mut nct_id: Vec<Option<String>> = Vec::new();
    let mut number_of_nsae_subjects: Vec<Option<i32>> = Vec::new();
    let mut minimum_age_num: Vec<Option<i32>> = Vec::new();
    let mut maximum_age_num: Vec<Option<i32>> = Vec::new();
    let mut design_groups: Vec<Option<String>> = Vec::new();
    let mut interventions: Vec<Option<String>> = Vec::new();
    let mut p_value: Vec<Option<String>> = Vec::new();
    let mut ci_percent: Vec<Option<String>> = Vec::new();
    let mut pmid: Vec<Option<String>> = Vec::new();
    let mut citation: Vec<Option<String>> = Vec::new();
    let mut recruitment_details: Vec<Option<String>> = Vec::new();
    let mut ae_count: Vec<Option<i64>> = Vec::new();
    let mut sae_count: Vec<Option<i64>> = Vec::new();
    let mut mortality_count: Vec<Option<i64>> = Vec::new();
    let mut num_aes_described: Vec<Option<i64>> = Vec::new();

    for row in vec {
        nct_id.push(row.nct_id.clone());
        number_of_nsae_subjects.push(row.number_of_nsae_subjects.clone());
        minimum_age_num.push(row.minimum_age_num.clone());
        maximum_age_num.push(row.maximum_age_num.clone());
        design_groups.push(row.design_groups.clone());
        interventions.push(row.interventions.clone());
        p_value.push(row.p_value.clone());
        ci_percent.push(row.ci_percent.clone());
        pmid.push(row.pmid.clone());
        citation.push(row.citation.clone());
        recruitment_details.push(row.recruitment_details.clone());
        ae_count.push(row.ae_count.clone());
        sae_count.push(row.sae_count.clone());
        mortality_count.push(row.mortality_count.clone());
        num_aes_described.push(row.num_aes_described.clone());
    }

    let df = df!(
        "nct_id" => &nct_id,
        "number_of_nsae_subjects" => &number_of_nsae_subjects,
        "minimum_age_num" => &minimum_age_num,
        "maximum_age_num" => &maximum_age_num,
        "design_groups" => &design_groups,
        "interventions" => &interventions,
        "p_value" => &p_value,
        "ci_percent" => &ci_percent,
        "pmid" => &pmid,
        "citation" => &citation,
        "recruitment_details" => &recruitment_details,
        "ae_count" => &ae_count,
        "sae_count" => &sae_count,
        "mortality_count" => &mortality_count,
        "num_aes_described" => &num_aes_described,
    )
    .unwrap();

    Ok(df)
}
