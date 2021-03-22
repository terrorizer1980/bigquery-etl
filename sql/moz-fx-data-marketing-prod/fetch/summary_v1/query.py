"""Load cost summary CSV into bigquery."""

import datetime

import click
from google.cloud import bigquery, storage

FILE_PREFIX = "mozilla_cost_summary_"


@click.command()
@click.option("--load-date", type=datetime.date.fromisoformat, required=True)
@click.option("--bucket-name", required=True)
@click.option("--dst-project", default="moz-fx-data-marketing-prod")
@click.option("--dst-dataset", default="fetch")
@click.option("--dst-table-name", default="summary_v1")
@click.option("--tmp-project", default="mozdata")
@click.option("--tmp-dataset", default="tmp")
@click.option("--tmp-table-name", default="iprospect_summary_load")
def load_file(
    load_date,
    dst_project,
    dst_dataset,
    dst_table_name,
    bucket_name,
    tmp_project,
    tmp_dataset,
    tmp_table_name,
):
    bq_client = bigquery.Client(project=dst_project)
    storage_client = storage.Client(project=dst_project)

    # filenames end with timestamp so use date to filter and then get latest file
    try:
        blob: storage.Blob = sorted(
            list(
                storage_client.list_blobs(
                    bucket_name, prefix=FILE_PREFIX + str(load_date)
                )
            ),
            key=lambda blob: blob.name,
            reverse=True,
        )[0]
    except IndexError:
        raise ValueError(f"No data found for {str(load_date)}")

    print(f"Found {blob.name}")

    tmp_table = bigquery.TableReference(
        bigquery.DatasetReference(tmp_project, tmp_dataset),
        tmp_table_name,
    )

    print(f"Creating table {tmp_table}")

    bq_client.load_table_from_uri(
        f"gs://{blob.bucket.name}/{blob.name}",
        tmp_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        ),
    ).result()

    dst_table = bigquery.TableReference(
        bigquery.DatasetReference(dst_project, dst_dataset), dst_table_name
    )

    transformation_query = f"""
    SELECT
        CAST('{load_date}' AS DATE) AS load_date,
        *,
    FROM
        {tmp_table}
    """

    print(f"Writing to table {dst_table}")

    bq_client.query(
        transformation_query,
        job_config=bigquery.QueryJobConfig(
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            destination=f"{dst_table}${load_date.strftime('%Y%m%d')}",
            time_partitioning=bigquery.TimePartitioning(field="load_date"),
        ),
    ).result()

    print(f"Deleting table {tmp_table}")

    bq_client.delete_table(tmp_table)


if __name__ == "__main__":
    load_file()
