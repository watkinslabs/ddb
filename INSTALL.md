# DDB Installation Guide

## Quick Install

### macOS/Linux (Homebrew)
```bash
# Add our tap
brew tap watkinslabs/ddb

# Install DDB
brew install ddb

# Enable shell completion
echo 'source <(ddb completion bash)' >> ~/.bashrc
# or for zsh
echo 'source <(ddb completion zsh)' >> ~/.zshrc
```

### Go Install
```bash
go install github.com/watkinslabs/ddb@latest
```

### Download Binary
```bash
# Linux AMD64
curl -L https://github.com/watkinslabs/ddb/releases/latest/download/ddb-linux-amd64 -o ddb
chmod +x ddb
sudo mv ddb /usr/local/bin/

# macOS AMD64
curl -L https://github.com/watkinslabs/ddb/releases/latest/download/ddb-darwin-amd64 -o ddb
chmod +x ddb
sudo mv ddb /usr/local/bin/

# macOS ARM64
curl -L https://github.com/watkinslabs/ddb/releases/latest/download/ddb-darwin-arm64 -o ddb
chmod +x ddb
sudo mv ddb /usr/local/bin/
```

## Package Managers

### Ubuntu/Debian (APT)
```bash
# Add our repository
curl -fsSL https://packages.watkinslabs.com/gpg.key | sudo apt-key add -
echo "deb https://packages.watkinslabs.com/apt stable main" | sudo tee /etc/apt/sources.list.d/watkinslabs.list

# Install
sudo apt update
sudo apt install ddb
```

### CentOS/RHEL/Fedora (YUM/DNF)
```bash
# Add our repository
sudo tee /etc/yum.repos.d/watkinslabs.repo <<EOF
[watkinslabs]
name=Watkins Labs Repository
baseurl=https://packages.watkinslabs.com/rpm
enabled=1
gpgcheck=1
gpgkey=https://packages.watkinslabs.com/gpg.key
EOF

# Install
sudo yum install ddb
# or for newer systems
sudo dnf install ddb
```

### Windows
```powershell
# Using Chocolatey
choco install ddb

# Using Scoop
scoop bucket add watkinslabs https://github.com/watkinslabs/scoop-bucket
scoop install ddb

# Manual install
# Download ddb-windows-amd64.exe from releases page
# Add to PATH
```

## Docker
```bash
# Run directly
docker run --rm -v $(pwd):/data watkinslabs/ddb:latest query "SELECT * FROM data" --file data:/data/file.csv

# Use as base image
FROM watkinslabs/ddb:latest
COPY data/ /data/
RUN ddb query "SELECT COUNT(*) FROM data" --file data:/data/input.csv
```

## Verification

After installation, verify DDB is working:

```bash
# Check version
ddb version

# Test with sample data
echo "name,age,city
Alice,25,NYC
Bob,30,SF" > test.csv

ddb query "SELECT name FROM data WHERE age > 25" --file data:test.csv
```

## Shell Completion

Enable shell completion for better CLI experience:

### Bash
```bash
# Temporary (current session)
source <(ddb completion bash)

# Permanent
echo 'source <(ddb completion bash)' >> ~/.bashrc
```

### Zsh
```bash
# Temporary (current session)  
source <(ddb completion zsh)

# Permanent
echo 'source <(ddb completion zsh)' >> ~/.zshrc
```

### Fish
```bash
ddb completion fish | source
```

### PowerShell
```powershell
ddb completion powershell | Out-String | Invoke-Expression
```

## Troubleshooting

### Permission Issues
```bash
# Make sure binary is executable
chmod +x ddb

# Check PATH
echo $PATH
which ddb
```

### File Access Issues
```bash
# Verify file permissions
ls -la your-data-file.csv

# Test with absolute path
ddb query "SELECT * FROM data" --file data:/absolute/path/to/file.csv
```

### Performance Issues
```bash
# Use verbose mode for diagnostics
ddb query "SELECT * FROM data" --file data:file.csv --verbose

# Adjust chunk size for large files
ddb query "SELECT * FROM data" --file data:file.csv --chunk-size 5000

# Enable parallel processing
ddb query "SELECT * FROM data" --file data:file.csv --parallel
```

## Uninstall

### Homebrew
```bash
brew uninstall ddb
brew untap watkinslabs/ddb
```

### Package Managers
```bash
# Ubuntu/Debian
sudo apt remove ddb

# CentOS/RHEL/Fedora
sudo yum remove ddb
```

### Manual
```bash
rm /usr/local/bin/ddb
```