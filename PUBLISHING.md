# Publishing DDB v0.1.0

This guide covers publishing the DDB package to crates.io and creating a GitHub release.

## Prerequisites

- [x] Git repository initialized
- [x] Initial commit created
- [x] Git tag v0.1.0 created
- [x] Cargo.toml metadata complete
- [x] CHANGELOG.md created
- [x] Package tested with `cargo package`
- [ ] GitHub repository created at watkinslabs/ddb
- [ ] crates.io account with API token

## Step 1: Push to GitHub

Push your code and tag to GitHub:

```bash
# Push main branch
git branch -M main
git push -u origin main

# Push the v0.1.0 tag
git push origin v0.1.0
```

## Step 2: Create GitHub Release

### Option A: Using GitHub CLI

```bash
gh release create v0.1.0 \
  --title "DDB v0.1.0" \
  --notes-file CHANGELOG.md \
  --latest
```

### Option B: Using GitHub Web Interface

1. Go to https://github.com/watkinslabs/ddb/releases/new
2. Select tag: `v0.1.0`
3. Title: `DDB v0.1.0`
4. Description: Copy contents from CHANGELOG.md
5. Mark as latest release
6. Click "Publish release"

## Step 3: Publish to crates.io

### 3.1 Login to crates.io

If not already logged in:

```bash
cargo login
# Paste your API token from https://crates.io/me
```

### 3.2 Publish the Package

```bash
# Dry run first (recommended)
cargo publish --dry-run

# Actual publish
cargo publish
```

**Note**: Publishing is permanent and cannot be undone. Package versions cannot be reused.

### 3.3 Verify Publication

After publishing (may take a few minutes):

```bash
# Search for your package
cargo search ddb-core

# Install from crates.io
cargo install ddb-core
```

Visit your package page: https://crates.io/crates/ddb-core

## Step 4: Update Documentation

After successful publication:

1. Update README.md to reference crates.io installation:
   ```bash
   # Add to Cargo.toml
   [dependencies]
   ddb-core = "0.1.0"
   ```

2. Add crates.io badge to README.md:
   ```markdown
   [![Crates.io](https://img.shields.io/crates/v/ddb-core.svg)](https://crates.io/crates/ddb-core)
   ```

## Important Notes

### License Considerations

This package uses the `CC-BY-NC-SA-4.0` license (Creative Commons Attribution-NonCommercial-ShareAlike).

**Important**: This is a non-commercial license. While crates.io accepts this license, users should be aware:
- ✅ Free to use for personal, educational, and non-commercial projects
- ❌ Cannot be used in commercial applications without permission
- ✅ Can modify and distribute under same license
- ✅ Must attribute the original author

### Package Size

The packaged crate is **27.1 MiB compressed** (123.6 MiB uncompressed).
This includes benchmark data files. Consider adding to `.gitignore` or `exclude` in Cargo.toml for future versions.

### Excluded Files

Currently excluded from the package:
- `benches/` - Benchmark suite
- `tests/fixtures/` - Test data

### Known Warnings

The package builds with 7 non-critical warnings:
- 4 unused imports (can be auto-fixed with `cargo fix`)
- 3 unused struct fields

These don't prevent publication but should be cleaned up in future versions.

## Verification Checklist

Before publishing, verify:

- [ ] All tests pass: `cargo test`
- [ ] Release build succeeds: `cargo build --release`
- [ ] Package builds: `cargo package`
- [ ] Documentation builds: `cargo doc --no-deps`
- [ ] Binary works: `./target/release/ddb version`
- [ ] MCP mode works: `./target/release/ddb --mcp` (press Ctrl+C to exit)
- [ ] CHANGELOG.md is up to date
- [ ] No sensitive information in code or docs
- [ ] License file present
- [ ] README.md includes installation instructions

## Post-Publication Tasks

1. Announce the release:
   - GitHub Discussions
   - Rust community forums
   - Social media

2. Monitor for issues:
   - GitHub Issues
   - crates.io package page

3. Plan next version:
   - Review CHANGELOG "Planned Features" section
   - Prioritize features for v0.2.0

## Troubleshooting

### "failed to verify" Error

If `cargo publish` fails verification:
```bash
# Clean and retry
cargo clean
cargo package
```

### License Warning

If crates.io warns about the license:
- The license is valid but ensure users understand non-commercial restrictions
- Consider adding a LICENSE.md file with clear terms

### Package Too Large

If exceeding crates.io size limits (10MB recommended, 50MB hard limit):
```bash
# Add to Cargo.toml [package] section:
exclude = [
    "benchmarks/data/*.csv",
    "examples/*.csv",
]
```

## Version Bumping for Future Releases

For the next version (v0.2.0):

```bash
# Update version in Cargo.toml
# Update CHANGELOG.md with new [0.2.0] section
# Commit changes
git add Cargo.toml CHANGELOG.md
git commit -m "Bump version to 0.2.0"

# Create new tag
git tag -a v0.2.0 -m "Release v0.2.0"

# Push
git push origin main v0.2.0

# Publish
cargo publish
```

## Support

For help with publishing:
- crates.io: https://doc.rust-lang.org/cargo/reference/publishing.html
- GitHub Releases: https://docs.github.com/en/repositories/releasing-projects-on-github

## Current Status

✅ **Ready to publish!**

- Package: ddb-core v0.1.0
- Git tag: v0.1.0
- Repository: git@github.com:watkinslabs/ddb.git
- License: CC-BY-NC-SA-4.0
- Size: 27.1 MiB compressed
