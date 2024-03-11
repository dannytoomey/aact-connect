use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;

pub async fn setup() -> Result<(), Box<dyn Error>> {
    if Path::new("./additional_data/query_5131_additional_data_2023-04-15.csv").exists() == false
        || Path::new("./comparisons/included_records.txt").exists() == false
        || Path::new("./query_text/systematic_review_query.txt").exists() == false
        || Path::new("./query_results").exists() == false
    {
        println!("Downloading required files...");
    }
    if Path::new("./additional_data/query_5131_additional_data_2023-04-15.csv").exists() == false {
        fs::create_dir_all("./additional_data")?;
        let file = "./additional_data/query_5131_additional_data_2023-04-15.csv";
        let url = "https://raw.githubusercontent.com/dannytoomey/aact-connect/master/additional_data/query_5131_additional_data_2023-04-15.csv"
            .to_string();
        let response = reqwest::get(&url).await?;
        let mut file = File::create(file)?;
        let content = response.bytes().await?;
        file.write_all(&content)?;
    }
    if Path::new("./comparisons/included_records.txt").exists() == false {
        fs::create_dir_all("./comparisons")?;
        let file = "./comparisons/included_records.txt";
        let url = "https://raw.githubusercontent.com/dannytoomey/aact-connect/master/comparisons/included_records.txt"
            .to_string();
        let response = reqwest::get(&url).await?;
        let mut file = File::create(file)?;
        let content = response.bytes().await?;
        file.write_all(&content)?;
    }
    if Path::new("./query_text/systematic_review_query.txt").exists() == false {
        fs::create_dir_all("./query_text")?;
        let file = "./query_text/systematic_review_query.txt";
        let url = "https://raw.githubusercontent.com/dannytoomey/aact-connect/master/query_text/systematic_review_query.txt"
            .to_string();
        let response = reqwest::get(&url).await?;
        let mut file = File::create(file)?;
        let content = response.bytes().await?;
        file.write_all(&content)?;
    }
    if Path::new("./query_results").exists() == false {
        fs::create_dir_all("./query_results")?;
    }
    Ok(())
}
