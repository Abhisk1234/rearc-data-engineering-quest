import boto3
import pandas as pd
import json
from io import StringIO

# S3 client pointing to LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://host.docker.internal:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

def analytics():
    # Load CSV
    csv_obj = s3.get_object(Bucket="bls-data", Key="pub/time.series/pr/pr.data.0.Current")
    csv_data = csv_obj["Body"].read().decode("utf-8")
    df_time_series = pd.read_csv(StringIO(csv_data), delimiter="\t")
    df_time_series.columns = df_time_series.columns.str.strip()

    # Load JSON
    json_obj = s3.get_object(Bucket="bls-data", Key="data.json")
    json_data = json.loads(json_obj["Body"].read())
    df_population = pd.json_normalize(json_data['data'])

    print(df_population.info())
    # Clean and filter population data
    df_population["Year"] = df_population["Year"].astype(int)
    df_population["Population"] = df_population["Population"].astype(int)

    pop_filtered = df_population[(df_population["Year"] >= 2013) & (df_population["Year"] <= 2018)]

    mean_pop = pop_filtered["Population"].mean()
    stddev_pop = pop_filtered["Population"].std()

    print("Mean Population (2013–2018):", mean_pop)
    print("Std Dev Population (2013–2018):", stddev_pop)


    # Clean data
    df_time_series["value"] = pd.to_numeric(df_time_series["value"], errors="coerce")
    df_time_series = df_time_series[df_time_series["period"].str.startswith("Q")]

    # Group and sum
    grouped = df_time_series.groupby(["series_id", "year"])["value"].sum().reset_index()

    # Find best year for each series_id
    best_years = grouped.loc[grouped.groupby("series_id")["value"].idxmax()].reset_index(drop=True)
    print(best_years)

    # Filter for specific series_id and period
    filtered_df = df_time_series[
        (df_time_series["series_id"].str.strip() == "PRS30006032") &
        (df_time_series["period"].str.strip() == "Q01")
    ].copy()

    filtered_df["year"] = filtered_df["year"].astype(int)
    filtered_df["value"] = pd.to_numeric(filtered_df["value"], errors="coerce")

    # Join with population
    df_population.rename(columns={"Year": "year", "Population": "population"}, inplace=True)
    joined_df = pd.merge(filtered_df, df_population[["year", "population"]], on="year", how="left")

    final_report = joined_df[["series_id", "year", "period", "value", "population"]]
    print(final_report)



def lambda_handler(event, context):
    analytics()

    return {
        "statusCode": 200,
        "body": "Analytics processed successfully"
    }
