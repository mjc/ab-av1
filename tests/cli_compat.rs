#![allow(unused_crate_dependencies)]

use std::process::Command;

fn run_ab_av1(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_ab-av1"))
        .args(args)
        .output()
        .expect("run ab-av1")
}

#[test]
fn top_level_help_mentions_all_commands() {
    let output = run_ab_av1(&["--help"]);
    assert!(output.status.success());

    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("Usage: ab-av1 <COMMAND>"));
    for cmd in [
        "sample-encode",
        "vmaf",
        "xpsnr",
        "encode",
        "crf-search",
        "auto-encode",
        "print-completions",
    ] {
        assert!(help.contains(cmd), "missing command: {cmd}");
    }
}

#[test]
fn crf_search_help_keeps_defaults_envs_and_value_names_visible() {
    let output = run_ab_av1(&["crf-search", "--help"]);
    assert!(output.status.success());

    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("--encoder <ENCODER>"));
    assert!(help.contains("--enc-input <ENC_INPUT_ARGS>"));
    assert!(help.contains("[env: AB_AV1_CACHE=]"));
    assert!(help.contains("[env: AB_AV1_TEMP_DIR=]"));
    assert!(help.contains("[default: 95]"));
    assert!(help.contains("[default: 80]"));
}

#[test]
fn bash_completion_contains_expected_command_paths() {
    let output = run_ab_av1(&["print-completions", "bash"]);
    assert!(output.status.success());

    let completions = String::from_utf8_lossy(&output.stdout);
    assert!(completions.contains("_ab_av1()"));
    assert!(completions.contains("ab__av1__subcmd__crf__subcmd__search"));
    assert!(completions.contains("ab__av1__subcmd__print__subcmd__completions"));
}
