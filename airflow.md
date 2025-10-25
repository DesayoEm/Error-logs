
**Date:** 22 Oct 2025
**Context:** Templating File Paths in Postgres COPY Commands with Airflow
**Version:** Airflow 3.0.6, PostgreSQL v15.1 

## Error
```bash
ERROR: syntax error at or near "/"
```

## Code (Before)
```sql
sql = '''
COPY page_views
FROM {{ti.xcom_pull(task_ids='process_page_views_count')}}
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"');
'''
```

## Solution
```sql
sql = '''
COPY page_views
FROM '{{ti.xcom_pull(task_ids='process_page_views_count')}}'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"');
'''
```

## Explanation

**Postgres sees the path as separate tokens, not a string**
Wrapping the Jinja template in single quotes ensure that after Airflow expands the template,
the resulting path is treated as a SQL string literal rather than bare tokens.

**Key takeaway**: When templating file paths or any string values in SQL, always wrap the Jinja
template expression in single quotes so the rendered result is a valid SQL string literal.




**Date:** 22 Oct 2025
**Context:** Templating File Paths in Postgres COPY Commands with Airflow
**Version:** Airflow 3.0.6, PostgreSQL v15.1 

## Error
```bash
ERROR: syntax error at or near "/"
```

## Code (Before)
```sql
sql = '''
COPY page_views
FROM {{ti.xcom_pull(task_ids='process_page_views_count')}}
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"');
'''
```

## Solution
```sql
sql = '''
COPY page_views
FROM '{{ti.xcom_pull(task_ids='process_page_views_count')}}'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '"');
'''
```

## Explanation

**Postgres sees the path as separate tokens, not a string**
Wrapping the Jinja template in single quotes ensure that after Airflow expands the template,
the resulting path is treated as a SQL string literal rather than bare tokens.

**Key takeaway**: When templating file paths or any string values in SQL, always wrap the Jinja
template expression in single quotes so the rendered result is a valid SQL string literal.

