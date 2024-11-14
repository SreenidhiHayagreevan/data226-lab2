{% snapshot lab2_snapshot %}
  {{
    config(
      target_schema='raw_data',
      target_database='dev',
      unique_key='date',
      strategy='check',
      check_cols=['open', 'high', 'low', 'close', 'volume']  
    )
  }}

  -- Snapshot query to capture data
  select * from DEV.RAW_DATA.LAB2

{% endsnapshot %}
