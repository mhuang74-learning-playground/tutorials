use structopt::StructOpt;
use failure::ResultExt;
use exitfailure::ExitFailure;

// search for a pattern in a file and display tghe lines that contain it

#[derive(StructOpt)]
struct Cli {

    // the pattern to look for
    pattern: String,
    // the path to the file to read
    #[structopt(parse(from_os_str))]
    path: std::path::PathBuf,
}

#[derive(Debug)]
struct CustomError(String);


fn main() -> Result<(), ExitFailure> {
    let args = Cli::from_args();

    let content = std::fs::read_to_string(&args.path)
        .with_context(|_| format!("could not read file `{}`", args.path.display()))?;

    find_matches(&content, &args.pattern, &mut std::io::stdout());

    Ok(())

}

fn find_matches(content: &str, pattern: &str, mut writer: impl std::io::Write) {

    for line in content.lines() {
        if line.contains(pattern) {
            writeln!(writer, "{}", line);
        }

    }
}



#[test]
fn find_a_match() {
    let mut result = Vec::new();
    find_matches("lorem ipsum\ndolor sit amet", "lorem", &mut result);
    assert_eq!(result, b"lorem ipsum\n");
}