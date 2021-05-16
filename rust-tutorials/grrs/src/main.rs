use structopt::StructOpt;
use anyhow::{Context, Result};

// search for a pattern in a file and display the lines that contain it

#[derive(Debug, StructOpt)]
#[structopt(name = "grrs", 
            about = "rust command-line tutorial: grrs",
            author = "mhuang74"
            )]
struct Cli {

    // the pattern to look for
    pattern: String,
    // the path to the file to read
    #[structopt(parse(from_os_str))]
    path: std::path::PathBuf,
}


fn main() -> Result<()> {
    let args = Cli::from_args();

    println!("args: {:?}", args);

    let content = std::fs::read_to_string(&args.path)
        .with_context(|| format!("could not read file `{}`", args.path.display()))?;

    grrs::find_matches(&content, &args.pattern, &mut std::io::stdout());

    Ok(())

}

