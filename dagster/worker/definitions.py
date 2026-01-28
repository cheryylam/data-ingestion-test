import numpy as np
from dagster import Definitions, asset, AssetExecutionContext, AssetKey
from dagster_dbt import DbtCliResource, dbt_assets
from resources import PostgresResource, manifest 

@asset(compute_kind = "python", key_prefix = "raw")
def wpp_project_data_xlsx(context: AssetExecutionContext, postgres_resource: PostgresResource):
    file_path = "/opt/dagster/app/modelling_exercise_raw_data.xlsx"
    table_name = "wpp_project_data_xlsx"
    
    context.log.info(f"Membaca file excel dari {file_path}")
    
    # Menggunakan fungsi read_excel yang baru kita buat
    df = postgres_resource.read_excel(file_path)
    
    # Menulis ke database postgres
    postgres_resource.write_table(df, table_name=table_name)
    
    context.log.info(f"Berhasil menulis {len(df)} baris ke table {table_name}")
    return df

# 2. Definisi Asset untuk dbt
@dbt_assets(
    manifest=manifest,
    select="*",
)
def dbts(context: AssetExecutionContext, dbt: DbtCliResource):
    run_arg = ["build"]

    yield from (
        dbt.cli(run_arg, context=context)
        .stream()
        .fetch_row_counts()
        .fetch_column_metadata()
    )

@asset(
    deps=AssetKey(["mart", "wpp_project_data_clean"]),
)
def top_ability_daily():
    pass

@asset(
    compute_kind="python",
    deps=[AssetKey(["wpp_project_data_clean"])],
    key_prefix = "mart"
)
def wpp_data_transformed(context: AssetExecutionContext, postgres_resource: PostgresResource):
    data = postgres_resource.read_table("SELECT * FROM mart.wpp_project_data_clean")

    vars_to_transform = [
        'sales', 'spend_on_meta', 'spend_on_tiktok', 
        'spend_on_tv', 'spend_on_ucontent', 'spend_on_youtube'
    ]

    for var in vars_to_transform:
        new_col_name = f'log_{var}'
        data[new_col_name] = np.log1p(data[var])
        context.log.info(f"Transformasi log selesai untuk: {var}")

    data = data.fillna(0)
    
    postgres_resource.write_table(
        data, 
        table_name="wpp_project_data_log", 
        schema="mart",
        if_exists="replace"
    )
    
    context.log.info("Asset wpp_data_transformed berhasil disimpan ke mart.wpp_project_data_log")
    return data

@asset(
    compute_kind="python",
    deps=[AssetKey(["mart", "wpp_data_transformed"])]
)
def wpp_report_excel(context: AssetExecutionContext, postgres_resource: PostgresResource):
    query = "SELECT * FROM mart.wpp_project_data_log"
    context.log.info("Menarik data log dari mart.wpp_project_data_log...")
    data = postgres_resource.read_table(query)

    output_path = "/opt/dagster/app/wpp_final_report.xlsx"

    context.log.info(f"Menulis data ke {output_path}...")
    data.to_excel(output_path, index=False, sheet_name='Log_Transformed_Data')
    
    context.log.info("File Excel berhasil dibuat!")
    
    return output_path

defs = Definitions(
    assets=[wpp_project_data_xlsx, dbts, wpp_data_transformed, wpp_report_excel],
    resources={
        "postgres_resource": PostgresResource(
            host="dwh",     
            username="dwhuser",
            password="dwhpass",
            database="warehouse",
            port=5432
        ),
        "dbt": DbtCliResource(project_dir="dbt"),
    },
)