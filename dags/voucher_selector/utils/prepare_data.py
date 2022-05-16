import os
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

pg_user = os.environ.get('POSTGRES_USER')
pg_password = os.environ.get('POSTGRES_PASSWORD')
pg_db = os.environ.get('POSTGRES_DB')
pg_host = os.environ.get('POSTGRES_HOST')
pg_port = os.environ.get('POSTGRES_PORT')


def prepare_vouchers_region(input_s3, input_tbl, output_tbl1, output_tbl2, schema, region='Peru'):

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # fetch data from customer_segments (job: load_pg_customer_segments)
    df_segments = hook.get_pandas_df(sql=f"select * from {schema}.{input_tbl}")

    df_segments_recency = df_segments[df_segments['segment_name'] == 'recency_segment']
    df_segments_frequent = df_segments[df_segments['segment_name'] == 'frequent_segment']

    # fetch voucher data from S3 parquet.gzip - will take a few mins.
    df = pd.read_parquet(path=input_s3)     # engine='pyarrow')
    df.reset_index(level=0, inplace=True)

    # cleanse
    df_region = df[df['country_code'] == region]
    df_region['voucher_amount'] = df_region['voucher_amount'].fillna(0)
    df_region['total_orders'] = pd.to_numeric(df_region['total_orders']).fillna(0)
    df_region['last_order_ts'] = pd.to_datetime(df_region['last_order_ts'])
    df_region['first_order_ts'] = pd.to_datetime(df_region['first_order_ts'])
    df_region['days_since_last_order'] = (pd.Timestamp.utcnow() - df_region['last_order_ts']).dt.days

    # map to recency segments
    df_segments_recency.index = pd.IntervalIndex.from_arrays(
        df_segments_recency['min_range'], df_segments_recency['max_range'], closed='both'
    )
    df_region['segment_type'] = df_region['days_since_last_order'].apply(
        lambda x: df_segments_recency.iloc[df_segments_recency.index.get_loc(x)]['segment_type']
    )
    df_region_recency = pd.merge(
        df_region, df_segments_recency, how='inner', left_on='segment_type', right_on='segment_type'
    )
    df_region.drop(columns=['segment_type'], inplace=True)

    # map to frequent segments
    df_segments_frequent.index = pd.IntervalIndex.from_arrays(
        df_segments_frequent['min_range'], df_segments_frequent['max_range'], closed='both'
    )
    df_region['segment_type'] = df_region['total_orders'].apply(
        lambda x: df_segments_frequent.iloc[df_segments_frequent.index.get_loc(x)]['segment_type']
    )
    df_region_frequent = pd.merge(
        df_region, df_segments_frequent, how='inner', left_on='segment_type', right_on='segment_type'
    )
    df_region.drop(columns=['segment_type'], inplace=True)

    df_region_recency_grouped = df_region_recency.groupby(
        ['min_range', 'max_range', 'segment_name', 'voucher_amount']).size().to_frame('count').reset_index()

    df_region_frequent_grouped = df_region_frequent.groupby(
        ['min_range', 'max_range', 'segment_name', 'voucher_amount']).size().to_frame('count').reset_index()

    df_region_grouped = df_region_recency_grouped.append(df_region_frequent_grouped)

    # save to postgres
    engine = hook.get_sqlalchemy_engine()   # conn = hook.get_conn()
    df_region_grouped.to_sql(name=output_tbl1, con=engine, schema=schema, if_exists='replace')

    #################################################

    # extra:
    df_region_recency.rename(columns={
        "segment_type": "recency_segment_type", "min_range": "recency_min",
        "max_range": "recency_max", "segment_name": "recency_segment"
    }, inplace=True)

    df_region_frequent.rename(columns={
        "segment_type": "frequent_segment_type", "min_range": "frequent_min",
        "max_range": "frequent_max", "segment_name": "frequent_segment"
    }, inplace=True)

    df_region = pd.merge(
        df_region_recency, df_region_frequent, how='inner',
        left_on=['index', 'timestamp', 'country_code', 'last_order_ts', 'first_order_ts', 'total_orders', 'voucher_amount', 'days_since_last_order'],
        right_on=['index', 'timestamp', 'country_code', 'last_order_ts', 'first_order_ts', 'total_orders', 'voucher_amount', 'days_since_last_order']
    )

    df_region.to_sql(name=output_tbl2, con=engine, schema=schema, if_exists='replace')
