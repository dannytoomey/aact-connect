use crate::structs::connect_results::ConnectResults;
use chrono;
use std::fs;
use tokio_postgres::{Error as TokioError, NoTls};

pub async fn get_results(user: &str, pw: &str) -> Result<Vec<ConnectResults>, TokioError> {
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
    let query = fs::read_to_string("./query_text/systematic_review_query.txt")
        .expect("query file not found")
        .replace("\n", " ");
    let rows = client.query(&query, &[]).await?;
    let mut results = Vec::<ConnectResults>::new();
    for row in rows {
        let nct_id: Option<String> = row.get("nct_id");
        let nlm_download_date_description: Option<String> =
            row.get("nlm_download_date_description");
        let study_first_submitted_date: Option<chrono::NaiveDate> =
            row.get("study_first_submitted_date");
        let results_first_submitted_date: Option<chrono::NaiveDate> =
            row.get("results_first_submitted_date");
        let disposition_first_submitted_date: Option<chrono::NaiveDate> =
            row.get("disposition_first_submitted_date");
        let last_update_submitted_date: Option<chrono::NaiveDate> =
            row.get("last_update_submitted_date");
        let study_first_submitted_qc_date: Option<chrono::NaiveDate> =
            row.get("study_first_submitted_qc_date");
        let study_first_posted_date: Option<chrono::NaiveDate> = row.get("study_first_posted_date");
        let study_first_posted_date_type: Option<String> = row.get("study_first_posted_date_type");
        let results_first_submitted_qc_date: Option<chrono::NaiveDate> =
            row.get("results_first_submitted_qc_date");
        let results_first_posted_date: Option<chrono::NaiveDate> =
            row.get("results_first_posted_date");
        let results_first_posted_date_type: Option<String> =
            row.get("results_first_posted_date_type");
        let disposition_first_submitted_qc_date: Option<chrono::NaiveDate> =
            row.get("disposition_first_submitted_qc_date");
        let disposition_first_posted_date: Option<chrono::NaiveDate> =
            row.get("disposition_first_posted_date");
        let disposition_first_posted_date_type: Option<String> =
            row.get("disposition_first_posted_date_type");
        let last_update_submitted_qc_date: Option<chrono::NaiveDate> =
            row.get("last_update_submitted_qc_date");
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
        let primary_completion_month_year: Option<String> =
            row.get("primary_completion_month_year");
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
        let expanded_access_type_individual: Option<bool> =
            row.get("expanded_access_type_individual");
        let expanded_access_type_intermediate: Option<bool> =
            row.get("expanded_access_type_intermediate");
        let expanded_access_type_treatment: Option<bool> =
            row.get("expanded_access_type_treatment");
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
        let plan_to_share_ipd_description: Option<String> =
            row.get("plan_to_share_ipd_description");
        let created_at: Option<chrono::NaiveDateTime> = row.get("created_at");
        let updated_at: Option<chrono::NaiveDateTime> = row.get("updated_at");
        let source_class: Option<String> = row.get("source_class");
        let delayed_posting: Option<String> = row.get("delayed_posting");
        let expanded_access_nctid: Option<String> = row.get("expanded_access_nctid");
        let expanded_access_status_for_nctid: Option<String> =
            row.get("expanded_access_status_for_nctid");
        let fdaaa801_violation: Option<bool> = row.get("fdaaa801_violation");
        let baseline_type_units_analyzed: Option<String> = row.get("baseline_type_units_analyzed");
        results.push(ConnectResults {
            nct_id,
            nlm_download_date_description,
            study_first_submitted_date,
            results_first_submitted_date,
            disposition_first_submitted_date,
            last_update_submitted_date,
            study_first_submitted_qc_date,
            study_first_posted_date,
            study_first_posted_date_type,
            results_first_submitted_qc_date,
            results_first_posted_date,
            results_first_posted_date_type,
            disposition_first_submitted_qc_date,
            disposition_first_posted_date,
            disposition_first_posted_date_type,
            last_update_submitted_qc_date,
            last_update_posted_date,
            last_update_posted_date_type,
            start_month_year,
            start_date_type,
            start_date,
            verification_month_year,
            verification_date,
            completion_month_year,
            completion_date_type,
            completion_date,
            primary_completion_month_year,
            primary_completion_date_type,
            primary_completion_date,
            target_duration,
            study_type,
            acronym,
            baseline_population,
            brief_title,
            official_title,
            overall_status,
            last_known_status,
            phase,
            enrollment,
            enrollment_type,
            source,
            limitations_and_caveats,
            number_of_arms,
            number_of_groups,
            why_stopped,
            has_expanded_access,
            expanded_access_type_individual,
            expanded_access_type_intermediate,
            expanded_access_type_treatment,
            has_dmc,
            is_fda_regulated_drug,
            is_fda_regulated_device,
            is_unapproved_device,
            is_ppsd,
            is_us_export,
            biospec_retention,
            biospec_description,
            ipd_time_frame,
            ipd_access_criteria,
            ipd_url,
            plan_to_share_ipd,
            plan_to_share_ipd_description,
            created_at,
            updated_at,
            source_class,
            delayed_posting,
            expanded_access_nctid,
            expanded_access_status_for_nctid,
            fdaaa801_violation,
            baseline_type_units_analyzed,
        });
    }
    Ok(results)
}
