#![allow(unused_crate_dependencies)]

use std::process::Command;

fn run_ab_av1(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_ab-av1"))
        .args(args)
        .output()
        .expect("run ab-av1")
}

#[test]
fn crf_search_validation_error_reaches_stderr() {
    let output = run_ab_av1(&["crf-search", "--input", "input.mkv", "--vmaf", "95"]);

    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(1));

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Error: Invalid use of --vmaf NUMBER"));
}

#[test]
fn encode_validation_error_reaches_stderr() {
    let output = run_ab_av1(&["encode", "--input", "input.mkv", "--crf", "30", "--enc", "-crf=32"]);

    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(1));

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Error: Encoder argument `-crf` not allowed"));
}
