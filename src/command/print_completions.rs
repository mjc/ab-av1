use clap::{CommandFactory, Parser};
use clap_complete::Shell;

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
    clap_complete::generate(
        shell,
        &mut crate::Command::command(),
        "ab-av1",
        &mut std::io::stdout(),
    );
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
