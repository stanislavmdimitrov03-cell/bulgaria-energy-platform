# Technical Decisions and Challenges

A honest account of the decisions I made while building this project 
and the problems I ran into along the way.

---

## Technical Decisions

### Why DuckDB instead of PostgreSQL or SQLite?
DuckDB runs as a single file with no server required. It's built 
specifically for analytical queries on columnar data — exactly what 
this project needs. PostgreSQL would have been overkill for a 
single-developer project, and SQLite isn't designed for analytics.

### Why Parquet instead of CSV?
Parquet stores data types properly — timestamps stay as timestamps, 
floats stay as floats. CSV stores everything as text and you have 
to convert on every load. Parquet is also 5-10x smaller and faster 
to read. The only downside is you can't open it in Notepad, which 
doesn't matter for a pipeline.

### Why a star schema instead of one big flat table?
A flat table would repeat "Sofia", 42.7065, 23.3219 eight thousand 
times. The star schema stores it once in dim_city and references it 
by a key. It also makes SQL queries cleaner and mirrors how real 
data warehouses are built — which was part of the point.

### Why Streamlit instead of Power BI or Tableau?
I already know Power BI and Tableau. Building in Streamlit meant 
learning something new.

### Why Lambda + EventBridge instead of a scheduled script on a server?
No server to maintain or pay for when idle. The pipeline runs for 
about 3 seconds per day and costs fractions of a cent. A server 
would run 24/7 and cost money even when doing nothing.

### Why separate IAM user instead of using root account?
Root account has unlimited power including billing. If those 
credentials were compromised it would be a serious problem. The 
IAM user has only the permissions needed for this project.

---

## Challenges I Faced

### ENTSO-E API token activation
The token option didn't appear in my account settings after 
registering. Turned out new accounts need manual activation via 
email. Sent an email to transparency@entsoe.eu and got access 
within a day.

### DuckDB temp directory error on Windows
DuckDB couldn't create its temp folder because Windows path 
handling with `..` in paths produces an invalid format. Fixed by 
wrapping all paths in `os.path.abspath()` and setting the temp 
directory explicitly to an absolute path.

### Accidentally committed the database file to GitHub
The DuckDB database file grew to 1.7GB and GitHub rejected the 
push. Had to add `*.db` to `.gitignore`, remove it from Git 
tracking with `git rm --cached`, and use `git filter-branch` to 
purge it from the commit history entirely.

### Git DNS resolution issue on Windows
Git couldn't resolve github.com even though the browser could. 
Fixed by running `git config --global http.sslBackend schannel` 
which tells Git to use Windows' built-in SSL instead of its own.

### Glue crawler not discovering curated tables
The Glue crawler kept grouping all curated Parquet files into one 
table instead of creating separate tables. Solved by manually 
creating the Athena tables using `CREATE EXTERNAL TABLE` SQL 
statements pointing to each file's S3 location directly.

### Python network issue with Open-Meteo
The requests library couldn't resolve the Open-Meteo hostname 
even though the browser could access it fine. Fixed by installing 
`python-certifi-win32` which makes Python use Windows' certificate 
store instead of its own.

### Star schema join returning no results
The core analytical query joining all three fact tables returned 
empty results. Diagnosed by printing sample date_keys from each 
table — the weather table had 5 identical keys per timestamp 
(one per city) which was causing the join to multiply incorrectly. 
Fixed by pre-filtering weather to a single city in a CTE before 
joining.

---

## What I Would Do Differently

- Add wind measurement stations near actual wind farm locations 
  on the Black Sea coast instead of using Sofia wind speed
- Automate the ENTSO-E generation fetch in Lambda alongside weather
- Add data quality checks that alert when incoming data looks wrong
- Extend coverage back to 2021-2022 to capture the European energy 
  crisis when prices spiked dramatically

