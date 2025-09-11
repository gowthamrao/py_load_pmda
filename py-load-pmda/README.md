# py-load-pmda

`py-load-pmda` is a Python package for robust, scalable, and automated Extraction, Transformation, and Loading (ETL) of regulatory data from the Japanese Pharmaceuticals and Medical Devices Agency (PMDA).

## Features

- **Modular ETL Framework:** Decoupled components for extraction, parsing, transformation, and loading.
- **Extensible Database Support:** Uses an adapter pattern to support different database backends, with native support for PostgreSQL.
- **High-Performance Loading:** Utilizes native database bulk loading mechanisms (e.g., PostgreSQL `COPY`) for maximum efficiency.
- **Robust Extraction:** Handles web scraping, file downloads, caching, and rate limiting.
- **Versatile Parsing:** Includes parsers for Excel, PDF tables, and JADER zip/CSV files.
- **Secure Configuration:** Supports environment variables for managing sensitive data like database credentials.

## Installation

The package can be installed from PyPI. It is recommended to use a virtual environment.

```bash
# Install the core package
pip install py-load-pmda

# To include support for PostgreSQL, install the 'postgres' extra
pip install py-load-pmda[postgres]
```

## Configuration

Configuration is managed through a combination of a `config.yaml` file and environment variables.

### 1. `config.yaml`

The package looks for a `config.yaml` file in the root of the project directory. This file defines the database connection (excluding password) and the settings for each dataset ETL pipeline.

An example `config.yaml` looks like this:
```yaml
database:
  type: "postgres"
  host: "localhost"
  port: 5432
  user: "admin"
  dbname: "pmda_db"

datasets:
  approvals:
    extractor: "ApprovalsExtractor"
    # ... and so on
```

### 2. Environment Variables (for Security)

For security, sensitive information like database credentials **must** be provided via environment variables. These variables override any corresponding values in `config.yaml`.

The supported environment variables are:
- `PMDA_DB_TYPE`
- `PMDA_DB_HOST`
- `PMDA_DB_PORT`
- `PMDA_DB_USER`
- `PMDA_DB_PASSWORD`
- `PMDA_DB_DBNAME`

**The `PMDA_DB_PASSWORD` is mandatory and must be set in your environment.**

### 3. Local Development (`.env` file)

For local development, you can create a `.env` file in the project root directory. The application will automatically load variables from this file.

**Example `.env` file:**
```
PMDA_DB_HOST=localhost
PMDA_DB_PORT=5432
PMDA_DB_USER=my_local_user
PMDA_DB_PASSWORD=my_secret_password
PMDA_DB_DBNAME=pmda_db
```
**Important:** Add `.env` to your `.gitignore` file to prevent committing secrets to version control.

## Usage

The package provides a Command-Line Interface (CLI) for running the ETL processes.

### Initialize the Database
This command creates the necessary schema and metadata tables in your target database.
```bash
py-load-pmda init-db
```

### Run an ETL Pipeline
Run the ETL for a specific dataset.
```bash
# Run the JADER pipeline (delta mode by default)
py-load-pmda run --dataset jader

# Run the approvals pipeline for a specific year (full refresh)
py-load-pmda run --dataset approvals --year 2024 --mode full

# Run the package inserts pipeline for a specific drug
py-load-pmda run --dataset package_inserts --drug-name "Loxonin"
```

### Check ETL Status
View the status of the latest runs for all datasets.
```bash
py-load-pmda status
```

### Validate Configuration
Check that the `config.yaml` file is valid and that a connection can be made to the database.
```bash
py-load-pmda check-config
```
