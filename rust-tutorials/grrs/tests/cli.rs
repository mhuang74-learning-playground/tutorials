use std::process::Command;
use assert_cmd::prelude::*;
use predicates::prelude::*;


#[test]
fn file_doesnt_exit() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("grrs")?;
    cmd.arg("foobar")
        .arg("test/file/doest/exist");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("No such file or directory"));

        Ok(())
}

use tempfile::NamedTempFile;
use std::io::{Write};

#[test]
fn find_content_in_file() -> Result<(), Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    writeln!(file, "A test\nActual content\nMore content\nAnother test")?;

    let mut cmd = Command::cargo_bin("grrs")?;
    cmd.arg("test")
        .arg(file.path());
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("test\nAnother test"));

        Ok(())
}
