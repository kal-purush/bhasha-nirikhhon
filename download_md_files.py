import gzip
from google.cloud import bigquery
import os

# Set up Google Cloud credentials
client = bigquery.Client()

# Define the SQL query
query = """
SELECT c.content, f.repo_name
FROM `bigquery-public-data.github_repos.contents` AS c
JOIN `bigquery-public-data.github_repos.files` AS f
ON c.id = f.id
WHERE f.path LIKE 'README%'
"""

# Run the query
query_job = client.query(query)

# Stream and save results to file
with gzip.open("readme_dump.txt.gz", "wt", encoding="utf-8") as f:
    for row in query_job:
        f.write(row.content + "\n\n")