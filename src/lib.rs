use std::fs;
use std::error::Error;
use std::path::Path;
use std::fs::File;
use std::io::{BufRead, BufReader};
use tokio_postgres::{NoTls, Error as TokioError};
use polars::prelude::*;
use polars::df;
use chrono;
use indicatif::{ProgressBar,ProgressStyle};
use clap::Parser;
use csv::Writer;

#[derive(Parser)]
pub struct Args{
    #[arg(short, long)]
    pub username: Option<String>,
    #[arg(short, long)]
    pub password: Option<String>,
    #[arg(short, long)]
    pub search: bool,
    #[arg(short, long)]
    pub current_frame: bool,
    #[arg(short, long)]
    pub existing_frame: Option<String>,
    #[arg(short, long, default_value_t = 100)]
    pub threads: u16
}

#[derive(Debug)]
pub struct ConnectResults{
	pub nct_id: Option<String>,
	pub nlm_download_date_description: Option<String>,
	pub study_first_submitted_date: Option<chrono::NaiveDate>,
	pub results_first_submitted_date: Option<chrono::NaiveDate>,
	pub disposition_first_submitted_date: Option<chrono::NaiveDate>,
	pub last_update_submitted_date: Option<chrono::NaiveDate>,
	pub study_first_submitted_qc_date: Option<chrono::NaiveDate>,
	pub study_first_posted_date: Option<chrono::NaiveDate>,
	pub study_first_posted_date_type: Option<String>,
	pub results_first_submitted_qc_date: Option<chrono::NaiveDate>,
	pub results_first_posted_date: Option<chrono::NaiveDate>,
	pub results_first_posted_date_type: Option<String>,
	pub disposition_first_submitted_qc_date: Option<chrono::NaiveDate>,
	pub disposition_first_posted_date: Option<chrono::NaiveDate>,
	pub disposition_first_posted_date_type: Option<String>,
	pub last_update_submitted_qc_date: Option<chrono::NaiveDate>,
	pub last_update_posted_date: Option<chrono::NaiveDate>,
	pub last_update_posted_date_type: Option<String>,
	pub start_month_year: Option<String>,
	pub start_date_type: Option<String>,
	pub start_date: Option<chrono::NaiveDate>,
	pub verification_month_year: Option<String>,
	pub verification_date: Option<chrono::NaiveDate>,
	pub completion_month_year: Option<String>,
	pub completion_date_type: Option<String>,
	pub completion_date: Option<chrono::NaiveDate>,
	pub primary_completion_month_year: Option<String>,
	pub primary_completion_date_type: Option<String>,
	pub primary_completion_date: Option<chrono::NaiveDate>,
	pub target_duration: Option<String>,
	pub study_type: Option<String>,
	pub acronym: Option<String>,
	pub baseline_population: Option<String>,
	pub brief_title: Option<String>,
	pub official_title: Option<String>,
	pub overall_status: Option<String>,
	pub last_known_status: Option<String>,
	pub phase: Option<String>,
	pub enrollment: Option<i32>,
	pub enrollment_type: Option<String>,
	pub source: Option<String>,
	pub limitations_and_caveats: Option<String>,
	pub number_of_arms: Option<i32>,
	pub number_of_groups: Option<i32>,
	pub why_stopped: Option<String>,
	pub has_expanded_access: Option<bool>,
	pub expanded_access_type_individual: Option<bool>,
	pub expanded_access_type_intermediate: Option<bool>,
	pub expanded_access_type_treatment: Option<bool>,
	pub has_dmc: Option<bool>,
	pub is_fda_regulated_drug: Option<bool>,
	pub is_fda_regulated_device: Option<bool>,
	pub is_unapproved_device: Option<bool>,
	pub is_ppsd: Option<bool>,
	pub is_us_export: Option<bool>,
	pub biospec_retention: Option<String>,
	pub biospec_description: Option<String>,
	pub ipd_time_frame: Option<String>,
	pub ipd_access_criteria: Option<String>,
	pub ipd_url: Option<String>,
	pub plan_to_share_ipd: Option<String>,
	pub plan_to_share_ipd_description: Option<String>,
	pub created_at: Option<chrono::NaiveDateTime>,
	pub updated_at: Option<chrono::NaiveDateTime>,
	pub source_class: Option<String>,
	pub delayed_posting: Option<String>,
	pub expanded_access_nctid: Option<String>,
	pub expanded_access_status_for_nctid: Option<String>,
	pub fdaaa801_violation: Option<bool>,
	pub baseline_type_units_analyzed: Option<String>
}

#[derive(Debug)]
pub struct AddResults{
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

pub fn check_args(mut args:Args) -> Result<Args,&'static str>{
    if Path::new("./private/myconfig.txt").exists() == false{
        if args.username == None || args.password == None{
            return Err("Please specify your crednentials with the arguments: 
    -u      - your AACT username 
    -p      - your AACT password

You can also create a directory named `private` and create a file named `myconfig.txt` in it, so that `./private/myconfig.txt` read as:
    <your username>
    <your password>
        ")
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
                _ => {}, 
             } 
        }
    }
    if args.search == false && args.current_frame == false && args.existing_frame == None {
        return Err("Please specify a command. Available options are:
    -s     - perform the query specified in ./query_text/
    -cef   - compare the orginal 4/15/23 results to a specified dataset in ./additional_data/
    ")
    } else {
        Ok(args)
    }
    
}

pub async fn get_results(user:&str, pw:&str) -> Result<Vec<ConnectResults>, TokioError>{
    let host = "aact-db.ctti-clinicaltrials.org";
    let port = 5432;
    let dbname = "aact";
    let user = user;
    let pw = pw;

    let conn = format!("host={host} user={user} password={pw} port={port} dbname={dbname}");
    
    let (client, connection) = tokio_postgres::connect(&conn, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let query = fs::read_to_string("./query_text/systematic_review_query.txt").expect("query file not found").replace("\n"," ");

    let rows = client
        .query(&query,&[])
        .await?;
    
    let mut results = Vec::<ConnectResults>::new();

    for row in rows {
        
        let nct_id: Option<String> = row.get("nct_id");
        let nlm_download_date_description: Option<String> = row.get("nlm_download_date_description");
        let study_first_submitted_date: Option<chrono::NaiveDate> = row.get("study_first_submitted_date"); 
        let results_first_submitted_date: Option<chrono::NaiveDate> = row.get("results_first_submitted_date"); 
        let disposition_first_submitted_date: Option<chrono::NaiveDate> = row.get("disposition_first_submitted_date"); 
        let last_update_submitted_date: Option<chrono::NaiveDate> = row.get("last_update_submitted_date"); 
        let study_first_submitted_qc_date: Option<chrono::NaiveDate> = row.get("study_first_submitted_qc_date"); 
        let study_first_posted_date: Option<chrono::NaiveDate> = row.get("study_first_posted_date"); 
        let study_first_posted_date_type: Option<String> = row.get("study_first_posted_date_type");
        let results_first_submitted_qc_date: Option<chrono::NaiveDate> = row.get("results_first_submitted_qc_date"); 
        let results_first_posted_date: Option<chrono::NaiveDate> = row.get("results_first_posted_date"); 
        let results_first_posted_date_type: Option<String> = row.get("results_first_posted_date_type");
        let disposition_first_submitted_qc_date: Option<chrono::NaiveDate> = row.get("disposition_first_submitted_qc_date"); 
        let disposition_first_posted_date: Option<chrono::NaiveDate> = row.get("disposition_first_posted_date"); 
        let disposition_first_posted_date_type: Option<String> = row.get("disposition_first_posted_date_type");
        let last_update_submitted_qc_date: Option<chrono::NaiveDate> = row.get("last_update_submitted_qc_date"); 
        let last_update_posted_date: Option<chrono::NaiveDate> = row.get("last_update_posted_date"); 
        let last_update_posted_date_type: Option<String> = row.get("last_update_posted_date_type");
        let start_month_year: Option<String> = row.get("start_month_year");
        let start_date_type: Option<String> = row.get("start_date_type");
        let start_date: Option<chrono::NaiveDate> = row.get("start_date"); 
        let verification_month_year: Option<String> = row.get("verification_month_year");
        let verification_date: Option<chrono::NaiveDate> = row.get("verification_date");         
        let completion_month_year: Option<String> = row.get("completion_month_year");
        let completion_date_type: Option<String> = row.get("completion_date_type");
        let completion_date: Option<chrono::NaiveDate> = row.get("completion_date"); 
        let primary_completion_month_year: Option<String> = row.get("primary_completion_month_year");
        let primary_completion_date_type: Option<String> = row.get("primary_completion_date_type");
        let primary_completion_date: Option<chrono::NaiveDate> = row.get("primary_completion_date"); 
        let target_duration: Option<String> = row.get("target_duration");
        let study_type: Option<String> = row.get("study_type");
        let acronym: Option<String> = row.get("acronym");
        let baseline_population: Option<String> = row.get("baseline_population");
        let brief_title: Option<String> = row.get("brief_title");
        let official_title: Option<String> = row.get("official_title");
        let overall_status: Option<String> = row.get("overall_status");
        let last_known_status: Option<String> = row.get("last_known_status");
        let phase: Option<String> = row.get("phase");
        let enrollment: Option<i32> = row.get("enrollment");
        let enrollment_type: Option<String> = row.get("enrollment_type");
        let source: Option<String> = row.get("source");
        let limitations_and_caveats: Option<String> = row.get("limitations_and_caveats");
        let number_of_arms: Option<i32> = row.get("number_of_arms");
        let number_of_groups: Option<i32> = row.get("number_of_groups");
        let why_stopped: Option<String> = row.get("why_stopped");
        let has_expanded_access: Option<bool> = row.get("has_expanded_access");
        let expanded_access_type_individual: Option<bool> = row.get("expanded_access_type_individual");
        let expanded_access_type_intermediate: Option<bool> = row.get("expanded_access_type_intermediate");
        let expanded_access_type_treatment: Option<bool> = row.get("expanded_access_type_treatment");
        let has_dmc: Option<bool> = row.get("has_dmc");
        let is_fda_regulated_drug: Option<bool> = row.get("is_fda_regulated_drug");
        let is_fda_regulated_device: Option<bool> = row.get("is_fda_regulated_device");
        let is_unapproved_device: Option<bool> = row.get("is_unapproved_device");
        let is_ppsd: Option<bool> = row.get("is_ppsd");
        let is_us_export: Option<bool> = row.get("is_us_export");
        let biospec_retention: Option<String> = row.get("biospec_retention");
        let biospec_description: Option<String> = row.get("biospec_description");
        let ipd_time_frame: Option<String> = row.get("ipd_time_frame");
        let ipd_access_criteria: Option<String> = row.get("ipd_access_criteria");
        let ipd_url: Option<String> = row.get("ipd_url");
        let plan_to_share_ipd: Option<String> = row.get("plan_to_share_ipd");
        let plan_to_share_ipd_description: Option<String> = row.get("plan_to_share_ipd_description");
        let created_at: Option<chrono::NaiveDateTime> = row.get("created_at"); 
        let updated_at: Option<chrono::NaiveDateTime> = row.get("updated_at"); 
        let source_class: Option<String> = row.get("source_class");
        let delayed_posting: Option<String> = row.get("delayed_posting");
        let expanded_access_nctid: Option<String> = row.get("expanded_access_nctid");
        let expanded_access_status_for_nctid: Option<String> = row.get("expanded_access_status_for_nctid");
        let fdaaa801_violation: Option<bool> = row.get("fdaaa801_violation");
        let baseline_type_units_analyzed: Option<String> = row.get("baseline_type_units_analyzed");

        results.push(ConnectResults {
            nct_id: nct_id,
            nlm_download_date_description,            
            study_first_submitted_date,
            results_first_submitted_date,
            disposition_first_submitted_date,
            last_update_submitted_date,
            study_first_submitted_qc_date,
            study_first_posted_date,
            study_first_posted_date_type: study_first_posted_date_type,
            results_first_submitted_qc_date,
            results_first_posted_date,
            results_first_posted_date_type: results_first_posted_date_type,
            disposition_first_submitted_qc_date: disposition_first_submitted_qc_date,
            disposition_first_posted_date: disposition_first_posted_date,
            disposition_first_posted_date_type: disposition_first_posted_date_type,
            last_update_submitted_qc_date: last_update_submitted_qc_date,
            last_update_posted_date: last_update_posted_date,
            last_update_posted_date_type: last_update_posted_date_type,
            start_month_year: start_month_year,
            start_date_type: start_date_type,
            start_date: start_date,
            verification_month_year: verification_month_year,
            verification_date: verification_date,
            completion_month_year: completion_month_year,
            completion_date_type: completion_date_type,
            completion_date: completion_date,
            primary_completion_month_year: primary_completion_month_year,
            primary_completion_date_type: primary_completion_date_type,
            primary_completion_date: primary_completion_date,
            target_duration: target_duration,
            study_type: study_type,
            acronym: acronym, 
            baseline_population: baseline_population,
            brief_title: brief_title,
            official_title: official_title,
            overall_status: overall_status,
            last_known_status: last_known_status,
            phase: phase,
            enrollment: enrollment,
            enrollment_type: enrollment_type,
            source: source,
            limitations_and_caveats: limitations_and_caveats,
            number_of_arms: number_of_arms,
            number_of_groups: number_of_groups,
            why_stopped: why_stopped,
            has_expanded_access: has_expanded_access,
            expanded_access_type_individual: expanded_access_type_individual,
            expanded_access_type_intermediate: expanded_access_type_intermediate,
            expanded_access_type_treatment: expanded_access_type_treatment,
            has_dmc: has_dmc,
            is_fda_regulated_drug: is_fda_regulated_drug,
            is_fda_regulated_device: is_fda_regulated_device,
            is_unapproved_device: is_unapproved_device,
            is_ppsd: is_ppsd,
            is_us_export: is_us_export,
            biospec_retention: biospec_retention,
            biospec_description: biospec_description,
            ipd_time_frame: ipd_time_frame,
            ipd_access_criteria: ipd_access_criteria,
            ipd_url: ipd_url,
            plan_to_share_ipd: plan_to_share_ipd,
            plan_to_share_ipd_description: plan_to_share_ipd_description,
            created_at: created_at,
            updated_at: updated_at,
            source_class,
            delayed_posting,
            expanded_access_nctid,
            expanded_access_status_for_nctid,
            fdaaa801_violation,
            baseline_type_units_analyzed
        });
    }    

    Ok(results)
    
}

pub fn result_struct_to_polars(vec: Vec<ConnectResults>) -> Result<DataFrame, Box<dyn Error>>{
    let mut nct_id : Vec<Option<String>>  = Vec::new();
    let mut nlm_download_date_description: Vec<Option<String>>  = Vec::new();
    let mut study_first_submitted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut results_first_submitted_date : Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut disposition_first_submitted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut last_update_submitted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut study_first_submitted_qc_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut study_first_posted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut study_first_posted_date_type: Vec<Option<String>>  = Vec::new();
    let mut results_first_submitted_qc_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut results_first_posted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut results_first_posted_date_type: Vec<Option<String>>  = Vec::new();
    let mut disposition_first_submitted_qc_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut disposition_first_posted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut disposition_first_posted_date_type: Vec<Option<String>>  = Vec::new();
    let mut last_update_submitted_qc_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut last_update_posted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut last_update_posted_date_type: Vec<Option<String>>  = Vec::new();
    let mut start_month_year: Vec<Option<String>>  = Vec::new();
    let mut start_date_type: Vec<Option<String>>  = Vec::new();
    let mut start_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut verification_month_year: Vec<Option<String>>  = Vec::new();
    let mut verification_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut completion_month_year: Vec<Option<String>>  = Vec::new();
    let mut completion_date_type: Vec<Option<String>>  = Vec::new();
    let mut completion_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut primary_completion_month_year: Vec<Option<String>>  = Vec::new();
    let mut primary_completion_date_type: Vec<Option<String>>  = Vec::new();
    let mut primary_completion_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut target_duration: Vec<Option<String>>  = Vec::new();
    let mut study_type: Vec<Option<String>>  = Vec::new();
    let mut acronym: Vec<Option<String>>  = Vec::new();
    let mut baseline_population: Vec<Option<String>>  = Vec::new();
    let mut brief_title: Vec<Option<String>>  = Vec::new();
    let mut official_title: Vec<Option<String>>  = Vec::new();
    let mut overall_status: Vec<Option<String>>  = Vec::new();
    let mut last_known_status: Vec<Option<String>>  = Vec::new();
    let mut phase: Vec<Option<String>>  = Vec::new();
    let mut enrollment: Vec<Option<i32>> = Vec::new();
    let mut enrollment_type: Vec<Option<String>>  = Vec::new();
    let mut source: Vec<Option<String>>  = Vec::new();
    let mut limitations_and_caveats: Vec<Option<String>>  = Vec::new();
    let mut number_of_arms: Vec<Option<i32>> = Vec::new();
    let mut number_of_groups: Vec<Option<i32>> = Vec::new();
    let mut why_stopped: Vec<Option<String>>  = Vec::new();
    let mut has_expanded_access: Vec<Option<bool>> = Vec::new();
    let mut expanded_access_type_individual: Vec<Option<bool>> = Vec::new();
    let mut expanded_access_type_intermediate: Vec<Option<bool>> = Vec::new();
    let mut expanded_access_type_treatment: Vec<Option<bool>> = Vec::new();
    let mut has_dmc: Vec<Option<bool>> = Vec::new();
    let mut is_fda_regulated_drug: Vec<Option<bool>> = Vec::new();
    let mut is_fda_regulated_device: Vec<Option<bool>> = Vec::new();
    let mut is_unapproved_device: Vec<Option<bool>> = Vec::new();
    let mut is_ppsd: Vec<Option<bool>> = Vec::new();
    let mut is_us_export: Vec<Option<bool>> = Vec::new();
    let mut biospec_retention: Vec<Option<String>>  = Vec::new();
    let mut biospec_description: Vec<Option<String>>  = Vec::new();
    let mut ipd_time_frame: Vec<Option<String>>  = Vec::new();
    let mut ipd_access_criteria: Vec<Option<String>>  = Vec::new();
    let mut ipd_url: Vec<Option<String>>  = Vec::new();
    let mut plan_to_share_ipd: Vec<Option<String>>  = Vec::new();
    let mut plan_to_share_ipd_description: Vec<Option<String>>  = Vec::new();
    let mut created_at: Vec<Option<chrono::NaiveDateTime>> = Vec::new();
    let mut updated_at: Vec<Option<chrono::NaiveDateTime>> = Vec::new();
    let mut source_class: Vec<Option<String>>  = Vec::new();
    let mut delayed_posting: Vec<Option<String>>  = Vec::new();
    let mut expanded_access_nctid: Vec<Option<String>>  = Vec::new();
    let mut expanded_access_status_for_nctid: Vec<Option<String>>  = Vec::new();
    let mut fdaaa801_violation: Vec<Option<bool>> = Vec::new();
    let mut baseline_type_units_analyzed: Vec<Option<String>>  = Vec::new();
    for row in vec{
        nct_id.push(row.nct_id.clone());
        nlm_download_date_description.push(row.nlm_download_date_description.clone());
        study_first_submitted_date.push(row.study_first_submitted_date.clone());
        results_first_submitted_date.push(row.results_first_submitted_date.clone());
        disposition_first_submitted_date.push(row.disposition_first_submitted_date.clone());
        last_update_submitted_date.push(row.last_update_submitted_date.clone());
        study_first_submitted_qc_date.push(row.study_first_submitted_qc_date.clone());
        study_first_posted_date.push(row.study_first_posted_date.clone());
        study_first_posted_date_type.push(row.study_first_posted_date_type.clone());
        results_first_submitted_qc_date.push(row.results_first_submitted_qc_date.clone());
        results_first_posted_date.push(row.results_first_posted_date.clone());
        results_first_posted_date_type.push(row.results_first_posted_date_type.clone());
        disposition_first_submitted_qc_date.push(row.disposition_first_submitted_qc_date.clone());
        disposition_first_posted_date.push(row.disposition_first_posted_date.clone());
        disposition_first_posted_date_type.push(row.disposition_first_posted_date_type.clone());
        last_update_submitted_qc_date.push(row.last_update_submitted_qc_date.clone());
        last_update_posted_date.push(row.last_update_posted_date.clone());
        last_update_posted_date_type.push(row.last_update_posted_date_type.clone());
        start_month_year.push(row.start_month_year.clone());
        start_date_type.push(row.start_date_type.clone());
        start_date.push(row.start_date.clone());
        verification_month_year.push(row.verification_month_year.clone());
        verification_date.push(row.verification_date.clone());
        completion_month_year.push(row.completion_month_year.clone());
        completion_date_type.push(row.completion_date_type.clone());
        completion_date.push(row.completion_date.clone());
        primary_completion_month_year.push(row.primary_completion_month_year.clone());
        primary_completion_date_type.push(row.primary_completion_date_type.clone());
        primary_completion_date.push(row.primary_completion_date.clone());
        target_duration.push(row.target_duration.clone());
        study_type.push(row.study_type.clone());
        acronym.push(row.acronym.clone());
        baseline_population.push(row.baseline_population.clone());
        brief_title.push(row.brief_title.clone());
        official_title.push(row.official_title.clone());
        overall_status.push(row.overall_status.clone());
        last_known_status.push(row.last_known_status.clone());
        phase.push(row.phase.clone());
        enrollment.push(row.enrollment.clone());
        enrollment_type.push(row.enrollment_type.clone());
        source.push(row.source.clone());
        limitations_and_caveats.push(row.limitations_and_caveats.clone());
        number_of_arms.push(row.number_of_arms.clone());
        number_of_groups.push(row.number_of_groups.clone());
        why_stopped.push(row.why_stopped.clone());
        has_expanded_access.push(row.has_expanded_access.clone());
        expanded_access_type_individual.push(row.expanded_access_type_individual.clone());
        expanded_access_type_intermediate.push(row.expanded_access_type_intermediate.clone());
        expanded_access_type_treatment.push(row.expanded_access_type_treatment.clone());
        has_dmc.push(row.has_dmc.clone());
        is_fda_regulated_drug.push(row.is_fda_regulated_drug.clone());
        is_fda_regulated_device.push(row.is_fda_regulated_device.clone());
        is_unapproved_device.push(row.is_unapproved_device.clone());
        is_ppsd.push(row.is_ppsd.clone());
        is_us_export.push(row.is_us_export.clone());
        biospec_retention.push(row.biospec_retention.clone());
        biospec_description.push(row.biospec_description.clone());
        ipd_time_frame.push(row.ipd_time_frame.clone());
        ipd_access_criteria.push(row.ipd_access_criteria.clone());
        ipd_url.push(row.ipd_url.clone());
        plan_to_share_ipd.push(row.plan_to_share_ipd.clone());
        plan_to_share_ipd_description.push(row.plan_to_share_ipd_description.clone());
        created_at.push(row.created_at.clone());
        updated_at.push(row.updated_at.clone());
        source_class.push(row.source_class.clone());
        delayed_posting.push(row.delayed_posting.clone());
        expanded_access_nctid.push(row.expanded_access_nctid.clone());
        expanded_access_status_for_nctid.push(row.expanded_access_status_for_nctid.clone());
        fdaaa801_violation.push(row.fdaaa801_violation.clone());
        baseline_type_units_analyzed.push(row.baseline_type_units_analyzed.clone());
    }

    let mut df = df!(
            "nct_id" => &nct_id,
            "nlm_download_date_description" => &nlm_download_date_description,
            "study_first_submitted_date" => &study_first_submitted_date,
            "results_first_submitted_date" => &results_first_submitted_date,
            "disposition_first_submitted_date" => &disposition_first_submitted_date,
            "last_update_submitted_date" => &last_update_submitted_date,
            "study_first_submitted_qc_date" => &study_first_submitted_qc_date,
            "study_first_posted_date" => &study_first_posted_date,
            "study_first_posted_date_type" => &study_first_posted_date_type,
            "results_first_submitted_qc_date" => &results_first_submitted_qc_date,
            "results_first_posted_date" => &results_first_posted_date,
            "results_first_posted_date_type" => &results_first_posted_date_type,
            "disposition_first_submitted_qc_date" => &disposition_first_submitted_qc_date,
            "disposition_first_posted_date" => &disposition_first_posted_date,
            "disposition_first_posted_date_type" => &disposition_first_posted_date_type,
            "last_update_submitted_qc_date" => &last_update_submitted_qc_date,
            "last_update_posted_date" => &last_update_posted_date,
            "last_update_posted_date_type" => &last_update_posted_date_type,
            "start_month_year" => &start_month_year,
            "start_date_type" => &start_date_type,
            "start_date" => &start_date,
            "verification_month_year" => &verification_month_year,
            "verification_date" => &verification_date,
            "completion_month_year" => &completion_month_year,
            "completion_date_type" => &completion_date_type,
            "completion_date" => &completion_date,
            "primary_completion_month_year" => &primary_completion_month_year,
            "primary_completion_date_type" => &primary_completion_date_type,
            "primary_completion_date" => &primary_completion_date,
            "target_duration" => &target_duration,
            "study_type" => &study_type,
            "acronym" => &acronym,
            "baseline_population" => &baseline_population,
            "brief_title" => &brief_title,
            "official_title" => &official_title,
            "overall_status" => &overall_status,
            "last_known_status" => &last_known_status,
            "phase" => &phase,
            "enrollment" => &enrollment,
            "enrollment_type" => &enrollment_type,
            "source" => &source,
            "limitations_and_caveats" => &limitations_and_caveats,
            "number_of_arms" => &number_of_arms,
            "number_of_groups" => &number_of_groups,
            "why_stopped" => &why_stopped,
            "has_expanded_access" => &has_expanded_access,
            "expanded_access_type_individual" => &expanded_access_type_individual,
            "expanded_access_type_intermediate" => &expanded_access_type_intermediate,
            "expanded_access_type_treatment" => &expanded_access_type_treatment,
            "has_dmc" => &has_dmc,
            "is_fda_regulated_drug" => &is_fda_regulated_drug,
            "is_fda_regulated_device" => &is_fda_regulated_device,
            "is_unapproved_device" => &is_unapproved_device,
            "is_ppsd" => &is_ppsd,
            "is_us_export" => &is_us_export,
            "biospec_retention" => &biospec_retention,
            "biospec_description" => &biospec_description,
            "ipd_time_frame" => &ipd_time_frame,
            "ipd_access_criteria" => &ipd_access_criteria,
            "ipd_url" => &ipd_url,
            "plan_to_share_ipd" => &plan_to_share_ipd,
            "plan_to_share_ipd_description" => &plan_to_share_ipd_description,
            "created_at" => &created_at,
            "updated_at" => &updated_at,
            "source_class" => &source_class,
            "delayed_posting" => &delayed_posting,
            "expanded_access_nctid" => &expanded_access_nctid,
            "expanded_access_status_for_nctid" => &expanded_access_status_for_nctid,
            "fdaaa801_violation" => &fdaaa801_violation,
            "baseline_type_units_analyzed" => &baseline_type_units_analyzed
        ).unwrap();
    let mut date = format!("{}",chrono::offset::Local::now());
    date = date[0..10].to_string();
    let path = format!("query_results/query_{}_results_{}.csv",df.shape().0,date);
    let mut file = std::fs::File::create(path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();
    Ok(df)

}

pub async fn add_results(user:&str, pw:&str, df: DataFrame, thread_num: usize) -> Result<Vec<AddResults>, Box<dyn Error>>{
    let host = "aact-db.ctti-clinicaltrials.org";
    let port = 5432;
    let dbname = "aact";
    let user = user;
    let pw = pw;

    let mut results = Vec::<AddResults>::new();

    let nct_vec: Vec<Option<&str>> = df.column("nct_id")?.utf8()?.into_iter().collect();

    let mut count = 0;
    let mut vec_vec_iter = Vec::<Vec<Option<&str>>>::new();

    for _ in 0..(nct_vec.len()/thread_num + 1){
        vec_vec_iter.push(Vec::<Option<&str>>::new());
    }

    for id in nct_vec.clone().into_iter(){
        vec_vec_iter[count/thread_num].push(id);
        count += 1;
    }

    let bar = ProgressBar::new(vec_vec_iter.len().try_into().unwrap());
        bar.set_style(ProgressStyle::with_template("{bar} ({percent}%) Elapsed: {elapsed_precise} Remaining: {eta_precise}")
            .unwrap()
            .progress_chars("##-"));


    for vec_iter in vec_vec_iter{
        let get_futures = vec_iter.into_iter().map(|id| async move {
            let conn = format!("host={host} user={user} password={pw} port={port} dbname={dbname}");
            let (client, connection) = tokio_postgres::connect(&conn, NoTls).await.unwrap();

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            
            let nct_id = id.unwrap();
            let query = format!("
    select cv.nct_id, cv.number_of_nsae_subjects, cv.minimum_age_num, cv.maximum_age_num, 
    dg.design_groups,
    iv.interventions,
    oap.p_value,oac.ci_percent,
    srp.pmid,src.citation,
    pf.recruitment_details,
    rd.AE_Count,rd.SAE_Count,rd.Mortality_Count,
    re.Num_AEs_described

    from (
    select calculated_values.nct_id, calculated_values.number_of_nsae_subjects, calculated_values.minimum_age_num, calculated_values.maximum_age_num
    from calculated_values 
    where calculated_values.nct_id = '{}' ) as cv
    left join (
    select design_groups.nct_id, string_agg(design_groups.description,'; ') as design_groups
    from design_groups 
    group by design_groups.nct_id) as dg
    on cv.nct_id = dg.nct_id
    left join (
    select interventions.nct_id, string_agg(interventions.description,'; ') as interventions
    from interventions 
    group by interventions.nct_id) as iv
    on cv.nct_id = iv.nct_id
    left join (
    select outcome_analyses.nct_id, string_agg(CAST(outcome_analyses.p_value as VarChar),'; ') as p_value
    from outcome_analyses 
    group by outcome_analyses.nct_id) as oap
    on cv.nct_id = oap.nct_id
    left join (
    select outcome_analyses.nct_id, string_agg(CAST(outcome_analyses.ci_percent as VarChar),'; ') as ci_percent
    from outcome_analyses 
    group by outcome_analyses.nct_id) as oac
    on cv.nct_id = oac.nct_id
    left join (
    select study_references.nct_id, string_agg(CAST(study_references.pmid as VarChar),'; ') as pmid
    from study_references 
    group by study_references.nct_id) as srp
    on cv.nct_id = srp.nct_id
    left join (
    select study_references.nct_id, string_agg(CAST(study_references.citation as VarChar),'; ') as citation
    from study_references 
    group by study_references.nct_id) as src
    on cv.nct_id = src.nct_id
    left join (
    select participant_flows.nct_id, string_agg(CAST(participant_flows.recruitment_details as VarChar),'; ') as recruitment_details
    from participant_flows 
    group by participant_flows.nct_id) as pf
    on cv.nct_id = pf.nct_id
    left join (
    select reported_events.nct_id, COUNT(DISTINCT reported_events.adverse_event_term) AS Num_AEs_described
    from reported_events 
    group by reported_events.nct_id) as re
    on cv.nct_id = re.nct_id

    left join(
    select reported_event_totals.nct_id,
    sum(case when reported_event_totals.classification = 'Total, other adverse events' then 
    reported_event_totals.subjects_affected else 0 end) as AE_Count,
    sum(case when reported_event_totals.classification = 'Total, serious adverse events' then 
    reported_event_totals.subjects_affected else 0 end) as SAE_Count,
    sum(case when reported_event_totals.classification = 'Total, all-cause mortality' then 
    reported_event_totals.subjects_affected else 0 end) as Mortality_Count
    from reported_event_totals
    group by reported_event_totals.nct_id) as rd
    on cv.nct_id = rd.nct_id",
        nct_id).replace("\n"," ").replace("\"","");
            let rows = client.query(&query,&[]).await.unwrap();
            rows
            
        });

        let out = futures::future::join_all(get_futures).await;

        for rows in &out {
            for row in rows{
                let nct_id: Option<String> = row.get("nct_id");
                let number_of_nsae_subjects: Option<i32> = row.get("number_of_nsae_subjects");
                let minimum_age_num: Option<i32> = row.get("minimum_age_num");
                let maximum_age_num: Option<i32> = row.get("maximum_age_num");
                let design_groups: Option<String> = row.get("design_groups");
                let interventions: Option<String> = row.get("interventions");
                let p_value: Option<String> = row.get("p_value");
                let ci_percent: Option<String> = row.get("ci_percent");
                let pmid: Option<String> = row.get("pmid");
                let citation: Option<String> = row.get("citation");
                let recruitment_details: Option<String> = row.get("recruitment_details");
                let ae_count: Option<i64> = row.get("ae_count");
                let sae_count: Option<i64> = row.get("sae_count");
                let mortality_count: Option<i64> = row.get("mortality_count");
                let num_aes_described: Option<i64> = row.get("num_aes_described");        

                let result = AddResults{
                    nct_id,
                    number_of_nsae_subjects,
                    minimum_age_num,
                    maximum_age_num,
                    design_groups,
                    interventions,   
                    p_value,
                    ci_percent,
                    pmid,
                    citation, 
                    recruitment_details,
                    ae_count,
                    sae_count,
                    mortality_count,
                    num_aes_described
                };
                results.push(result);
                
            }   
        }

        bar.inc(1);

    }

    Ok(results)
    
}

pub fn add_struct_to_polars(vec: Vec<AddResults>) -> Result<DataFrame, Box<dyn Error>>{
    let mut nct_id : Vec<Option<String>>  = Vec::new();
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
    
    for row in vec{
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
        ).unwrap();

    Ok(df)

}

pub fn compare_dataset(comp_df: DataFrame)->Result<(),Box<dyn Error>>{
    let orginal_df = CsvReader::from_path("additional_data/query_5131_additional_data_2023-04-15.csv")?
        .has_header(true)
        .finish()
        .unwrap();
    
    let orgn_binding = orginal_df.clone();
    let comp_binding = comp_df.clone();

    let orgn_shape = orgn_binding.shape();
    let comp_shape = comp_binding.shape();

    if orgn_shape.0 == comp_shape.0 {
        println!(" 1. Datasets have same number of rows");
    } else {
        println!(" 1. Datasets DO NOT have same number of rows:
    Orginal dataset:    {} rows
    Comparison dataset: {} rows",orgn_shape.0,comp_shape.0);
    }

    let orgn_nct_vec: Vec<Option<&str>> = orgn_binding.column("nct_id")?.utf8()?.into_iter().collect();
    let comp_nct_vec: Vec<Option<&str>> = comp_binding.column("nct_id")?.utf8()?.into_iter().collect();
    let mut shared_nct_vec = Vec::<Option<&str>>::new();

    let orgn_nct_vec_iter = orgn_nct_vec.iter();
    let mut msg = 0;
    for val in orgn_nct_vec_iter {
        if comp_nct_vec.contains(&val) {
            shared_nct_vec.push(*val)
        } else {
            if msg == 0 {
                println!("\n 2. Trials present in the orginal results not present in the current comparison:");
                msg = 1
            }
            println!("  - {}", val.unwrap());
        }
    }
    let comp_nct_vec_iter = comp_nct_vec.iter();
    msg = 0;
    for val in comp_nct_vec_iter {
        if orgn_nct_vec.contains(&val) {
        } else {
            if msg == 0 {
                println!("\n    Trials present in the current comparison not present in the orginal results:");
                msg = 1
            }
            println!("  - {}", val.unwrap());
        }
    }

    let ids_series = Series::new("shared_id", shared_nct_vec);
    let filter_expr = col("nct_id").is_in(lit(ids_series));
    let mut ob_binding = orgn_binding.lazy().filter(filter_expr.clone()).collect().unwrap().drop("Unnamed: 0").unwrap();
    let orgn_filtered = ob_binding.sort_in_place(["nct_id"],true,true);
    let mut cb_binding = comp_binding.lazy().filter(filter_expr.clone()).collect().unwrap();
    let comp_filtered = cb_binding.sort_in_place(["nct_id"],true,true);
    
    
    let mut date = format!("{}",chrono::offset::Local::now());
    date = date[0..10].to_string();
    let path = format!("comparisons/CT_SR_search_comparions_{}.csv",date);
    let mut wtr = Writer::from_path(path.clone())?;
    wtr.write_record(&["nct_id","column","4/15/23 value","Current search value","Included in systematic review?"])?;

    let of_binding = orgn_filtered?.clone();
    let cf_binding = comp_filtered?.clone();

    let mut orgn_iter = of_binding.iter();
    let mut comp_iter = cf_binding.iter();

    let filter_shape = of_binding.shape();
    let col_names = of_binding.get_column_names();
    let shared_ncts : Vec<Option<&str>> = of_binding.column("nct_id")?.utf8()?.into_iter().collect();

    let mut changes = 0;
    let mut inc_changes = 0;

    let mut included_ncts = Vec::<String>::new();
    let filename = "./comparisons/included_records.txt";
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);
    for (_, line) in reader.lines().enumerate() {
        included_ncts.push(line?);
    }

    for col in 0..filter_shape.1 {
        let orgn_series = orgn_iter.next().unwrap();
        let comp_series = comp_iter.next().unwrap().cast(orgn_series.dtype()).unwrap();
        let length = orgn_series.len();
        
        for i in 0..length {    

            let orgn_val = orgn_series.get(i).unwrap();
            let comp_val = comp_series.get(i).unwrap();
            
            if orgn_val != comp_val {
                let orgn_str: &str = &String::from(format!("{}",orgn_val));
                let comp_str: &str = &String::from(format!("{}",comp_val));
                if orgn_str == "\"True\"" && comp_str == "\"true\"" {
                } else {
                    wtr.serialize((shared_ncts[i], col_names[col], orgn_str, comp_str, included_ncts.contains(&String::from(shared_ncts[i].unwrap()))))?;
                    wtr.flush()?;
                    changes += 1;
                    if included_ncts.contains(&String::from(shared_ncts[i].unwrap())) {
                        inc_changes += 1;
                    }
                }
            }   
        }   


             
    }
    
    println!("\n 3. {} changes logged at {}", changes, path);
    println!("    {} changes noted for records that were included in the systematic review \n", inc_changes);

    Ok(())

}









