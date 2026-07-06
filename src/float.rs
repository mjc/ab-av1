/// f32 wrapper that displays minimal decimal places.
#[derive(Debug, Clone, Copy)]
pub struct TerseF32(pub f32);

impl std::fmt::Display for TerseF32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if pseudo_int(self.0.into()) {
            write!(f, "{:.0}", self.0)
        } else if pseudo_int(f64::from(self.0) * 10.0) {
            write!(f, "{:.1}", self.0)
        } else if pseudo_int(f64::from(self.0) * 100.0) {
            write!(f, "{:.2}", self.0)
        } else {
            self.0.fmt(f)
        }
    }
}

#[inline]
fn pseudo_int(f: f64) -> bool {
    !(0.0002..=0.9998).contains(&f.fract())
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;
    use rstest::rstest;

    mod proptest_terse_f32 {
        use super::*;

        proptest! {
            #[test]
            fn display_never_panics(v in any::<f32>()) {
                // setup
                let terse = TerseF32(v);

                // execute
                let formatted = format!("{terse}");

                // assert
                prop_assert!(!formatted.is_empty() || v.is_nan());
            }
        }
    }

    #[rstest]
    #[case(1.0, "1")]
    #[case(1.5, "1.5")]
    #[case(2.0, "2")]
    #[case(2.25, "2.25")]
    #[case(0.9999, "0.9999")] // ab-kgc.68: just below 1.0 must not display as integer 1
    #[case(0.999, "0.999")]
    #[case(-1.5, "-1.5")] // ab-kgc.69: negative values must preserve sign
    #[case(-0.25, "-0.25")]
    fn terse_f32_display(#[case] value: f32, #[case] expected: &str) {
        // setup
        let terse = TerseF32(value);

        // execute
        let formatted = format!("{terse}");

        // assert
        assert_eq!(formatted, expected);
    }
}
