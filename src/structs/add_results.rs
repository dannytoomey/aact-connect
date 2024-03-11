#[derive(Debug)]
pub struct AddResults {
    pub nct_id: Option<String>,
    pub number_of_nsae_subjects: Option<i32>,
    pub minimum_age_num: Option<i32>,
    pub maximum_age_num: Option<i32>,
    pub design_groups: Option<String>,
    pub interventions: Option<String>,
    pub p_value: Option<String>,
    pub ci_percent: Option<String>,
    pub pmid: Option<String>,
    pub citation: Option<String>,
    pub recruitment_details: Option<String>,
    pub ae_count: Option<i64>,
    pub sae_count: Option<i64>,
    pub mortality_count: Option<i64>,
    pub num_aes_described: Option<i64>,
}
