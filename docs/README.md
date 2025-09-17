# DDB Documentation Site

This directory contains the complete documentation site for DDB, designed for GitHub Pages deployment.

## Site Structure

```
docs/
├── index.html          # Main documentation homepage
├── functions.html      # Complete SQL functions reference  
├── examples.html       # Comprehensive examples and tutorials
├── _config.yml        # Jekyll/GitHub Pages configuration
└── README.md          # This file
```

## Features Documented

### Core Capabilities
- ✅ **30+ SQL Functions** - Complete function library with examples
- ✅ **44 Passing Tests** - Full test suite validation
- ✅ **Pattern Matching** - LIKE with wildcards, IS NULL/IS NOT NULL
- ✅ **Complex Expressions** - Functions in all SQL clauses
- ✅ **Multi-Format Support** - CSV, JSON, JSONL, YAML, Parquet
- ✅ **Performance Optimization** - Parallel processing, memory efficiency

### Function Categories Covered
- **String Functions**: UPPER, LOWER, LENGTH, SUBSTR, CONCAT, TRIM, LEFT, RIGHT, REPLACE, REVERSE, REPEAT
- **Math Functions**: ABS, ROUND, FLOOR, CEIL, MOD, POWER, SQRT, SIGN
- **Date/Time Functions**: NOW, DATE, YEAR, MONTH, DAY
- **Utility Functions**: COALESCE, ISNULL/IFNULL
- **Pattern Matching**: LIKE with % and _ wildcards
- **NULL Testing**: IS NULL, IS NOT NULL

### Advanced Features
- **Expression Complexity**: Functions in WHERE, JOIN, SELECT clauses
- **Column Aliases**: AS keyword support
- **Complex JOINs**: Multi-table operations with expressions
- **Parallel Processing**: Configurable workers and chunk sizes
- **Memory Efficiency**: Streaming processing for large files

## GitHub Pages Deployment

### Option 1: Direct HTML Deployment
The site is built as static HTML and can be deployed directly to GitHub Pages:

1. **Enable GitHub Pages** in your repository settings
2. **Set source** to "Deploy from a branch" 
3. **Select branch** `main` and folder `/docs`
4. **Access your site** at `https://watkinslabs.github.io/ddb/`

### Option 2: Jekyll Integration
The site includes Jekyll configuration for enhanced GitHub Pages features:

1. **Push to GitHub** - The `_config.yml` enables Jekyll processing
2. **GitHub Pages will automatically** build and deploy the site
3. **SEO and sitemap** are automatically generated

## Customization

### Update Repository Information
Edit `docs/_config.yml`:
```yaml
github_username: watkinslabs
repository: ddb
title: Your Project Name
description: Your project description
```

### Add Analytics
Add your Google Analytics tracking ID:
```yaml
google_analytics: UA-XXXXXXXX-X
```

### Customize Styling
The sites use inline CSS for easy customization. Key elements:
- **Color scheme**: `#667eea` (primary blue) and `#764ba2` (secondary purple)
- **Typography**: System fonts with monospace for code
- **Layout**: Responsive grid system
- **Components**: Cards, stats, function grids

## Content Highlights

### Main Page (`index.html`)
- Executive overview with key statistics
- Feature showcase with performance metrics
- Quick start examples
- Complete installation guide

### Functions Reference (`functions.html`)
- Detailed documentation for all 30+ functions
- Syntax examples and parameter descriptions
- Pattern matching guide with visual examples
- Interactive function cards with usage tips

### Examples & Tutorials (`examples.html`)
- Real-world scenarios and use cases
- Progressive complexity from basic to advanced
- Multi-format data examples
- Performance optimization examples
- Output format demonstrations

## Local Development

To preview the site locally:

```bash
# Simple HTTP server
cd docs
python -m http.server 8000
# Visit http://localhost:8000

# Or with Jekyll (if installed)
bundle exec jekyll serve
# Visit http://localhost:4000
```

## Site Statistics

The documentation comprehensively covers:
- **44 test cases** with examples
- **30+ SQL functions** with full documentation  
- **5 file formats** supported
- **Multiple output formats** (table, CSV, JSON, YAML)
- **Performance tuning** guidelines
- **Real-world examples** and use cases

This creates a professional, comprehensive documentation site that showcases the full capabilities of DDB and provides users with everything they need to get started and become proficient with the tool.