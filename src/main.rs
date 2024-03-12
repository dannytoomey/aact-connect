use clap::Parser;
use polars::prelude::*;
use std::error::Error;
use std::process;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _ = aact_connect::setup::setup::setup().await?;
    let mut args = aact_connect::structs::args::Args::parse();
    args = match aact_connect::setup::check_args::check_args(args) {
        Ok(args) => args,
        Err(err) => {
            eprintln!("Error processing arguments: {err}");
            process::exit(1);
        }
    };
    if args.search {
        println!("Performing query...");
        let get_results = aact_connect::get_results::get_results::get_results(
            args.username.as_deref().unwrap(),
            args.password.as_deref().unwrap(),
        )
        .await?;
        println!("Storing results as dataframe...");
        let df = aact_connect::get_results::result_struct_to_polars::result_struct_to_polars(
            get_results,
            false,
        )?;
        let threads_used = args.threads;
        println!(
            "Adding additional results using {} threads...",
            threads_used
        );
        let add_results = aact_connect::add_results::add_results::add_results(
            args.username.as_deref().unwrap(),
            args.password.as_deref().unwrap(),
            df.clone(),
            threads_used.into(),
        )
        .await?;
        println!("Storing additional results as dataframe...");
        let add_df =
            aact_connect::add_results::add_struct_to_polars::add_struct_to_polars(add_results)?;
        let mut combined_df = df.left_join(&add_df, ["nct_id"], ["nct_id"])?;
        let mut date = format!("{}", chrono::offset::Local::now());
        date = date[0..10].to_string();
        let path = format!(
            "additional_data/query_{}_additional_data_{}.csv",
            combined_df.shape().0,
            date
        );
        let mut file = std::fs::File::create(path.clone()).unwrap();
        CsvWriter::new(&mut file).finish(&mut combined_df).unwrap();
        println!("  - Dataset saved at {}", path.clone());
        if args.current_frame {
            println!("Comparing datasets...");
            let mut partial_schema: Schema = Schema::new();
            partial_schema.with_column("ci_percent".into(), DataType::Utf8);
            let comp_df = CsvReader::from_path(path.clone())?
                .has_header(true)
                .with_dtypes(Some(Arc::new(partial_schema)))
                .finish()
                .unwrap();
            let _ = aact_connect::compare_dataset::compare_dataset(comp_df);
        }
    }
    if args.existing_frame != None {
        println!("Comparing datasets...");
        let mut partial_schema: Schema = Schema::new();
        partial_schema.with_column("ci_percent".into(), DataType::Utf8);
        let comp_df = CsvReader::from_path(args.existing_frame.unwrap())?
            .has_header(true)
            .with_dtypes(Some(Arc::new(partial_schema)))
            .finish()
            .unwrap();
        let _ = aact_connect::compare_dataset::compare_dataset(comp_df);
    }
    Ok(())
}
