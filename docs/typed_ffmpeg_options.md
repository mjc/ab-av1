# Typed ffmpeg option boundary

ab-av1 keeps the user-facing shell surface in `clap`, but the ffmpeg lowering layer is typed for the options the project owns.

## Owned fragments

- Encoder selection and defaults: `Encoder`, `EncoderArg`, `EncoderInputArg`, `SvtArg`
- Owned invariants: `PixelFormat`, `KeyInterval`, `Crf`, `MinScore`, `SampleDuration`, `FrameRateOverride`
- Encoded arg rendering: `ffmpeg::FfmpegEncodeArgs` and `ffmpeg::VCodecSpecific`

## Boundary

- Typed values are used for ab-av1-owned options and defaults.
- Pass-through args stay pass-through so ffmpeg/version-specific options do not need a database.
- Reserved flags are rejected at the CLI/rules boundary before ffmpeg command assembly.

## Lowering shape

- `clap` parses the shell input into normalized config structs.
- Normalized config lowers into typed ffmpeg arg fragments.
- Typed fragments render into one ffmpeg command path for sample encode and final encode.

## Non-goals

- Do not enumerate all ffmpeg options.
- Do not make `clap` own ffmpeg's open-ended option space.
- Do not duplicate the rule engine inside the shell parser.
