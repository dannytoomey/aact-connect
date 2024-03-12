use chrono;
use csv::Writer;
use polars::prelude::*;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};

pub fn compare_dataset(comp_df: DataFrame) -> Result<(), Box<dyn Error>> {
    let orginal_df =
        CsvReader::from_path("additional_data/query_5131_additional_data_2023-04-15.csv")?
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
        println!(
            " 1. Datasets DO NOT have same number of rows:
    Orginal dataset:    {} rows
    Comparison dataset: {} rows",
            orgn_shape.0, comp_shape.0
        );
    }

    let orgn_nct_vec: Vec<Option<&str>> =
        orgn_binding.column("nct_id")?.utf8()?.into_iter().collect();
    let comp_nct_vec: Vec<Option<&str>> =
        comp_binding.column("nct_id")?.utf8()?.into_iter().collect();
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
    let mut ob_binding = orgn_binding
        .lazy()
        .filter(filter_expr.clone())
        .collect()
        .unwrap()
        .drop("Unnamed: 0")
        .unwrap();
    let orgn_filtered = ob_binding.sort_in_place(["nct_id"], true, true);
    let mut cb_binding = comp_binding
        .lazy()
        .filter(filter_expr.clone())
        .collect()
        .unwrap();
    let comp_filtered = cb_binding.sort_in_place(["nct_id"], true, true);

    let mut date = format!("{}", chrono::offset::Local::now());
    date = date[0..10].to_string();
    let path = format!("comparisons/CT_SR_search_comparions_{}.csv", date);
    let mut wtr = Writer::from_path(path.clone())?;
    wtr.write_record(&[
        "nct_id",
        "column",
        "4/15/23 value",
        "Current search value",
        "Included in systematic review?",
    ])?;

    let of_binding = orgn_filtered?.clone();
    let cf_binding = comp_filtered?.clone();

    let mut orgn_iter = of_binding.iter();
    let mut comp_iter = cf_binding.iter();

    let filter_shape = of_binding.shape();
    let col_names = of_binding.get_column_names();
    let shared_ncts: Vec<Option<&str>> = of_binding.column("nct_id")?.utf8()?.into_iter().collect();

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
                let orgn_str: &str = &String::from(format!("{}", orgn_val));
                let comp_str: &str = &String::from(format!("{}", comp_val));
                if orgn_str == "\"True\"" && comp_str == "\"true\"" {
                } else {
                    wtr.serialize((
                        shared_ncts[i],
                        col_names[col],
                        orgn_str,
                        comp_str,
                        included_ncts.contains(&String::from(shared_ncts[i].unwrap())),
                    ))?;
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
    println!(
        "    {} changes noted for records that were included in the systematic review \n",
        inc_changes
    );
    Ok(())
}
