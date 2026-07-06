use clap::{CommandFactory, Parser};
use clap_complete::Shell;
use std::io::Write;

/// Print shell completions.
#[derive(Parser)]
#[group(skip)]
pub struct Args {
    /// Shell.
    #[arg(value_enum, default_value_t = Shell::Bash)]
    shell: Shell,
}

#[derive(Debug, Clone, Copy)]
pub struct PrintCompletionsConfig {
    shell: Shell,
}

impl From<Args> for PrintCompletionsConfig {
    fn from(Args { shell }: Args) -> Self {
        Self { shell }
    }
}

pub fn print_completions(config: PrintCompletionsConfig) {
    let PrintCompletionsConfig { shell } = config;
    let mut completions = Vec::new();
    clap_complete::generate(
        shell,
        &mut crate::Command::command(),
        "ab-av1",
        &mut completions,
    );
    if matches!(shell, Shell::Bash) {
        print!(
            "{}",
            String::from_utf8_lossy(&completions).replace("_ab-av1", "_ab_av1")
        );
    } else {
        let _ = std::io::stdout().write_all(&completions);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_lowers_to_print_completions_config() {
        let args = Args { shell: Shell::Zsh };
        let config = PrintCompletionsConfig::from(args);

        assert!(matches!(config.shell, Shell::Zsh));
    }
}
