from data_diff import diff_tables, connect_to_table
import os
from typing import Iterator, Tuple


def compare(source_table, target_table, key_cols, extra_columns: Tuple[str, ...] = None, **bq_config) -> Iterator:
  """
  bq_config: dict: Could be consist of 'project', 'dataset' (mandatory) and 'keyfile' (optional) Seervice account path
  """
  if ['project', 'dataset'] not in bq_config:
    raise Exception('You must define BigQuery project and dataset in bq_config parameter')
  
  db_conf = {
     'driver': 'bigquery', 
     **bq_config
  }

  target = connect_to_table(
      db_conf,
      table_name=f'{bq_config["dataset"]}.target',
      key_columns=key_cols
  )
  
  source = connect_to_table(
      db_conf,
      table_name=f'{bq_config["dataset"]}.source',
      key_columns=key_cols
  )
  
  return diff_tables(
      source,
      target,
      key_columns=key_cols,
      extra_columns=extra_columns
  )
  
