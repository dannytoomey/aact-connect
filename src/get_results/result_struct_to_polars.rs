use crate::structs::connect_results::ConnectResults;
use chrono;
use polars::df;
use polars::prelude::*;
use std::error::Error;

pub fn result_struct_to_polars(
    vec: Vec<ConnectResults>,
    test: bool,
) -> Result<DataFrame, Box<dyn Error>> {
    let mut nct_id: Vec<Option<String>> = Vec::new();
    let mut nlm_download_date_description: Vec<Option<String>> = Vec::new();
    let mut study_first_submitted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut results_first_submitted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut disposition_first_submitted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut last_update_submitted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut study_first_submitted_qc_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut study_first_posted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut study_first_posted_date_type: Vec<Option<String>> = Vec::new();
    let mut results_first_submitted_qc_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut results_first_posted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut results_first_posted_date_type: Vec<Option<String>> = Vec::new();
    let mut disposition_first_submitted_qc_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut disposition_first_posted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut disposition_first_posted_date_type: Vec<Option<String>> = Vec::new();
    let mut last_update_submitted_qc_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut last_update_posted_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut last_update_posted_date_type: Vec<Option<String>> = Vec::new();
    let mut start_month_year: Vec<Option<String>> = Vec::new();
    let mut start_date_type: Vec<Option<String>> = Vec::new();
    let mut start_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut verification_month_year: Vec<Option<String>> = Vec::new();
    let mut verification_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut completion_month_year: Vec<Option<String>> = Vec::new();
    let mut completion_date_type: Vec<Option<String>> = Vec::new();
    let mut completion_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut primary_completion_month_year: Vec<Option<String>> = Vec::new();
    let mut primary_completion_date_type: Vec<Option<String>> = Vec::new();
    let mut primary_completion_date: Vec<Option<chrono::NaiveDate>> = Vec::new();
    let mut target_duration: Vec<Option<String>> = Vec::new();
    let mut study_type: Vec<Option<String>> = Vec::new();
    let mut acronym: Vec<Option<String>> = Vec::new();
    let mut baseline_population: Vec<Option<String>> = Vec::new();
    let mut brief_title: Vec<Option<String>> = Vec::new();
    let mut official_title: Vec<Option<String>> = Vec::new();
    let mut overall_status: Vec<Option<String>> = Vec::new();
    let mut last_known_status: Vec<Option<String>> = Vec::new();
    let mut phase: Vec<Option<String>> = Vec::new();
    let mut enrollment: Vec<Option<i32>> = Vec::new();
    let mut enrollment_type: Vec<Option<String>> = Vec::new();
    let mut source: Vec<Option<String>> = Vec::new();
    let mut limitations_and_caveats: Vec<Option<String>> = Vec::new();
    let mut number_of_arms: Vec<Option<i32>> = Vec::new();
    let mut number_of_groups: Vec<Option<i32>> = Vec::new();
    let mut why_stopped: Vec<Option<String>> = Vec::new();
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
    let mut biospec_retention: Vec<Option<String>> = Vec::new();
    let mut biospec_description: Vec<Option<String>> = Vec::new();
    let mut ipd_time_frame: Vec<Option<String>> = Vec::new();
    let mut ipd_access_criteria: Vec<Option<String>> = Vec::new();
    let mut ipd_url: Vec<Option<String>> = Vec::new();
    let mut plan_to_share_ipd: Vec<Option<String>> = Vec::new();
    let mut plan_to_share_ipd_description: Vec<Option<String>> = Vec::new();
    let mut created_at: Vec<Option<chrono::NaiveDateTime>> = Vec::new();
    let mut updated_at: Vec<Option<chrono::NaiveDateTime>> = Vec::new();
    let mut source_class: Vec<Option<String>> = Vec::new();
    let mut delayed_posting: Vec<Option<bool>> = Vec::new();
    let mut expanded_access_nctid: Vec<Option<String>> = Vec::new();
    let mut expanded_access_status_for_nctid: Vec<Option<String>> = Vec::new();
    let mut fdaaa801_violation: Vec<Option<bool>> = Vec::new();
    let mut baseline_type_units_analyzed: Vec<Option<String>> = Vec::new();
    for row in vec {
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
    )
    .unwrap();
    if test == false {
        let mut date = format!("{}", chrono::offset::Local::now());
        date = date[0..10].to_string();
        let path = format!("query_results/query_{}_results_{}.csv", df.shape().0, date);
        let mut file = std::fs::File::create(path).unwrap();
        CsvWriter::new(&mut file).finish(&mut df).unwrap();
    }
    Ok(df)
}
