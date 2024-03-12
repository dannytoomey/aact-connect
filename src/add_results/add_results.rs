use crate::structs::add_results::AddResults;
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use std::error::Error;
use tokio_postgres::NoTls;

pub async fn add_results(
    user: &str,
    pw: &str,
    df: DataFrame,
    thread_num: usize,
) -> Result<Vec<AddResults>, Box<dyn Error>> {
    let host = "aact-db.ctti-clinicaltrials.org";
    let port = 5432;
    let dbname = "aact";
    let user = user;
    let pw = pw;

    let mut results = Vec::<AddResults>::new();
    let nct_vec: Vec<Option<&str>> = df.column("nct_id")?.utf8()?.into_iter().collect();
    let mut count = 0;
    let mut vec_vec_iter = Vec::<Vec<Option<&str>>>::new();

    for _ in 0..(nct_vec.len() / thread_num + 1) {
        vec_vec_iter.push(Vec::<Option<&str>>::new());
    }

    for id in nct_vec.clone().into_iter() {
        vec_vec_iter[count / thread_num].push(id);
        count += 1;
    }

    let bar = ProgressBar::new(vec_vec_iter.len().try_into().unwrap());
    bar.set_style(
        ProgressStyle::with_template(
            "{bar} ({percent}%) Elapsed: {elapsed_precise} Remaining: {eta_precise}",
        )
        .unwrap()
        .progress_chars("##-"),
    );

    for vec_iter in vec_vec_iter {
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
            for row in rows {
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

                let result = AddResults {
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
                    num_aes_described,
                };
                results.push(result);
            }
        }
        bar.inc(1);
    }
    Ok(results)
}
