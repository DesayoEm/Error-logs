# PyArrow 21.0.0 Repetition Level Histogram Bug

**Date:** 09 Nov 2025  
**Context:** Writing clinical trials API response data (nested JSON) to Parquet files for S3 storage. Files were being written successfully with correct schema metadata, but completely unreadable with "Repetition level histogram size mismatch" errors.  
**Version:** PyArrow 21.0.0, pandas 2.x, Python 3.11

## Error
```python
OSError: Repetition level histogram size mismatch

Traceback (most recent call last):
  File "~\anaconda3\Lib\site-packages\pyarrow\parquet\core.py", line 467, in read_row_group
    return self.reader.read_row_group(i, column_indices=column_indices,
  File "~\anaconda3\Lib\site-packages\pyarrow\_parquet.pyx", line 1655, in pyarrow._parquet.ParquetReader.read_row_group
  File "~\anaconda3\Lib\site-packages\pyarrow\_parquet.pyx", line 1691, in pyarrow._parquet.ParquetReader.read_row_groups
  File "~\anaconda3\Lib\site-packages\pyarrow\error.pxi", line 92, in pyarrow.lib.check_status
OSError: Repetition level histogram size mismatch
```

## Code (Before)
```python
def save_response(self, data: Dict, bucket_destination: str):
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)

    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    bucket_name = config.CTGOV_STAGING_BUCKET
    key = f"{bucket_destination}/{self.current_page}.parquet"

    self.s3.load_bytes(
        bytes_data=buffer.getvalue(),
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
```

**Debugging notes:**
- File metadata showed correct schema: `studies: string, nextPageToken: string`
- Schema inspection with `pf.schema_arrow` worked fine
- File metadata accessible with `pf.metadata`

HOWEVER:

- Any attempt to read actual data failed with repetition level error
- Error occurred even when forcing all columns to strings with `df.astype(str)`
- Error persisted across different read methods (pandas, PyArrow, row groups)
- Error happened with both buffer writes and direct file writes
- problem occurred with both S3 and local file storage

## Solution
```bash
# downgrade to stable PyArrow version
pip uninstall pyarrow
pip install pyarrow==17.0.0
```

### What caused the error
PyArrow 21.0.0 has a known bug in how it writes Parquet files with string columns. The bug manifests in the **data pages** (not metadata/schema), 
causing corruption in the repetition level histograms - internal Parquet structures used to encode nested/repeated data.
The bug was introduced in PyArrow 21.0.0's refactoring of string handling and will likely be fixed in future releases.


Files written with PyArrow 21.0.0 are **permanently corrupted** and must be regenerated with a stable version.
