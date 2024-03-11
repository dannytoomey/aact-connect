use clap::Parser;

#[derive(Parser)]
pub struct Args {
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
    #[arg(short, long, default_value_t = 64)]
    pub threads: u16,
}
