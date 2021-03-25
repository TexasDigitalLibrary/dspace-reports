# dspace-reports

A python3-based tool to generate and email statistical reports for [DSpace](https://github.com/DSpace/DSpace) repository administrators.

## Requirements

-----

- Python 3.6+
- PostgreSQL 9.6+
- DSpace 6.x repository **

** If your Solr index contains statistics from legacy DSpace 5.x or earlier instances, then the quality of the reports will go up significantly if you have migrated the old statistics to the new UUID identifiers in DSpace 6. [See the DSpace Wiki for more information](https://wiki.lyrasis.org/display/DSDOC6x/SOLR+Statistics+Maintenance#SOLRStatisticsMaintenance-UpgradeLegacyDSpaceObjectIdentifiers(pre-6xstatistics)toDSpace6xUUIDIdentifiers)

## Python 3 Virtual Environment Setup

-----

```bash
python3 -m venv venv
source venv/bin/activate
pip install pipenv
pipenv install
```

## Configuration

-----

```bash
cp config/application.yml.sample config/application.yml
```

### Example

```yaml
dspace_name: 'MyDSpace'
dspace_server: 'http://localhost:8080'
solr_server: 'http://localhost:8080/solr'
oai_server: 'http://localhost:8080/oai'
rest_server: 
    url: 'http://localhost:8080/rest'
    username: 'admin@example.org'
    password: 'password'
statistics_db:
    host: 'localhost'
    port: '5432'
    name: 'dspace_statistics'
    username: 'dspace_statistics'
    password: 'dspace_statistics'
work_dir: '/tmp'
log_path: 'logs'
log_file: 'statistics-reports.log'
log_level: 'INFO'
smtp_host: 'localhost'
smtp_auth: 'tls'
smtp_port: 25
smtp_username: 'username'
smtp_password: 'password'
from_email: 'admin@example.org'
admin_emails:
        - email1
        - email2
```

Configure application.yml according to your particular environment. The admin_emails list in the configuration refers to the email addresses that will receive the stats reports if the email flag is set when running `run_reports.py` or `run_cron.py` (see below).

## Usage

-----

**NOTE: All of the following commands assume that the user is in the virutal environment.**

### Database

There are several ways to generate statistical reports with this tool. They all begin with the database manager script that allows the user to create, drop and recreate the database tables to store metadata and statistics.

```bash
Usage: database_manager.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIG_FILE, --config=CONFIG_FILE
                        Configuration file
  -f FUNCTION, --function=FUNCTION
                        Database function to perform. Options: create, drop,
                        check, recreate
```

For example, the first time stats are generated the user should run:

```bash
python database_manager.py -c config/application.yml -f create
```

And then after that the database tables can be recreated before running the stats generation process again.

```bash
python database_manager.py -c config/application.yml -f recreate
```

### Indexing

With a fresh database, the user can generate stats reports for the entire repository with `run_indexer.py`.

```bash
Usage: run_indexer.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIG_FILE, --config=CONFIG_FILE
                        Configuration file
  -o OUTPUT_DIR, --output_dir=OUTPUT_DIR
                        Directory for results files.
```

There is another option to generate statistics separately for communiities, collections, and items. They all generally take the form of:

```bash
python run_community_indexer.py -c config/application.py -o /tmp/reports
```

### Reports

When all indexing is complete and the metadata and stats are in the database, it's time to generate Excel reports. This can be done with `run_reports.py`.

```bash
Usage: run_reports.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIG_FILE, --config=CONFIG_FILE
                        Configuration file
  -o OUTPUT_DIR, --output_dir=OUTPUT_DIR
                        Directory for results files.
  -e, --email           Send email with stats reports to admin(s)?
```

For example:

```bash
python run_reports.py -c config/application.yml -o /tmp/reports -e  
```

### Cron job

In order to facilitate generating stastical reports on a regular basis, the indexing and reports processes have been combined into a single script `run_cron.py` that runs in a similar way to the other scripts.

```bash
Usage: run_cron.py [options]

Options:
  -h, --help            show this help message and exit
  -c CONFIG_FILE, --config=CONFIG_FILE
                        Configuration file
  -o OUTPUT_DIR, --output_dir=OUTPUT_DIR
                        Directory for results files.
  -e, --email           Send email with stats reports to admin(s)?
```

For example:

```bash
python run_cron.py -c config/application.yml -o /tmp/reports -e  
```

## License

-----

This code is licensed under the [GNU General Public License (GPL) V3](https://www.gnu.org/licenses/gpl-3.0.en.html).

**NOTE: Special thanks to the [DSpace Statistics API](https://github.com/ilri/dspace-statistics-api) project from which the Solr queries for views and downloads in this project are based.**
