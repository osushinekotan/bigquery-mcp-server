from google.cloud import bigquery

from servers.schemas.bigquery import ColumnDetails, Dataset, QueryResult, Table, TableDetails, TableSchema
from servers.settings import ALLOWED_DATASETS, ALLOWED_STATEMENTS, MAX_BYTES_BILLED, PROJECT_ID


class BigqueryTool:
    def __init__(self, client: bigquery.Client):
        self.client = client

    def list_datasets(self) -> list[Dataset]:
        """
        List all datasets in the BigQuery project.

        If ALLOWED_DATASETS is configured, only returns those datasets.
        """
        try:
            datasets = list(self.client.list_datasets())

            if not datasets:
                return []

            result = []
            for dataset in datasets:
                dataset_id = dataset.dataset_id

                # Filter by allowed datasets if configured
                if ALLOWED_DATASETS is not None and dataset_id not in ALLOWED_DATASETS:
                    continue

                result.append(Dataset(dataset_id=dataset_id))

            return result
        except Exception as e:
            # NOTE: なんのエラーかは要検討
            raise ValueError(f"Error listing datasets: {str(e)}") from e

    def get_allowed_datasets(self):
        """
        Returns the allowed datasets configured in the environment.
        """
        try:
            if not ALLOWED_DATASETS:
                return [Dataset(dataset_id="*")]

            result = []
            for dataset_id in ALLOWED_DATASETS:
                result.append(Dataset(dataset_id=dataset_id))

            return result
        except Exception as e:
            raise ValueError(f"Error getting allowed datasets: {str(e)}") from e

    async def list_tables(self, dataset_id: str | None) -> list[Table]:
        """
        List tables in BigQuery project, optionally filtered by dataset.

        Args:
            dataset_id: Optional dataset ID to filter tables

        Returns:
            List of tables in the specified dataset or all datasets if not specified.
        """
        try:
            # Check if dataset is allowed if filtering is applied
            if dataset_id and ALLOWED_DATASETS is not None and dataset_id not in ALLOWED_DATASETS:
                raise ValueError(f"Dataset {dataset_id} is not allowed.")

            tables = []

            # List tables based on dataset filter
            if dataset_id:
                dataset_ref = self.client.dataset(dataset_id)
                bq_tables = list(self.client.list_tables(dataset_ref))
                print(f"# bq_tables: {len(bq_tables)}")

                for table in bq_tables:
                    tables.append(Table(table_id=table.table_id, dataset_id=dataset_id))
            else:
                # If no dataset specified, list tables from all allowed datasets
                datasets_to_query = (
                    ALLOWED_DATASETS
                    if ALLOWED_DATASETS is not None
                    else [ds.dataset_id for ds in self.client.list_datasets()]
                )

                for ds_id in datasets_to_query:
                    dataset_ref = self.client.dataset(ds_id)
                    bq_tables = list(self.client.list_tables(dataset_ref))
                    print(f"# bq_tables: {len(bq_tables)}")

                    for table in bq_tables:
                        tables.append(Table(table_id=table.table_id, dataset_id=ds_id))

            return tables
        except Exception as e:
            raise ValueError(f"Error listing tables: {str(e)}") from e

    async def describe_table(self, dataset_id: str, table_id: str) -> TableDetails:
        """
        Get detailed information about a specific table using INFORMATION_SCHEMA.

        Args:
            dataset_id: Dataset ID
            table_id: Table ID
        Returns:
            TableDetails object containing metadata about the table.
        """
        try:
            # Check if dataset is allowed
            if ALLOWED_DATASETS is not None and dataset_id not in ALLOWED_DATASETS:
                raise ValueError(f"Dataset {dataset_id} is not allowed.")

            # Query INFORMATION_SCHEMA.TABLES for table metadata
            schema_query = f"""
            SELECT
            DATE(TIMESTAMP_MILLIS(creation_time)) AS creation_date,
            DATE(TIMESTAMP_MILLIS(last_modified_time)) AS last_modified_date,
            row_count,
            size_bytes
            FROM
            `{PROJECT_ID}.{dataset_id}.__TABLES__`
            WHERE
            table_id = '{table_id}'
            """

            table_info = None
            for row in self.client.query(schema_query).result():
                table_info = row
                break

            if not table_info:
                raise ValueError(f"Table {table_id} not found in dataset {dataset_id}.")

            # Get column details
            column_query = f"""
            SELECT
                column_name,
                is_nullable,
                data_type,
                is_partitioning_column
            FROM `{PROJECT_ID}.{dataset_id}`.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{table_id}'
            """
            column_details = self.client.query(column_query).result()
            columns = []
            for row in column_details:
                columns.append(
                    ColumnDetails(
                        column_name=row.column_name,
                        is_nullable=row.is_nullable,
                        data_type=row.data_type,
                        is_partitioning_column=row.is_partitioning_column,
                    )
                )

            # Create response object
            table_details = TableDetails(
                table_id=table_id,
                dataset_id=dataset_id,
                columns=columns,
                row_count=table_info.row_count,
                size_bytes=table_info.size_bytes,
                size_gbytes=table_info.size_bytes / (1024 * 1024 * 1024),
                created=str(table_info.creation_date) if table_info.creation_date else None,
                last_modified=str(table_info.last_modified_date) if table_info.last_modified_date else None,
            )

            return table_details

        except Exception as e:
            raise ValueError(f"Error describing table {table_id} in dataset {dataset_id}: {str(e)}") from e

    async def execute_query(self, query: str, dry_run: bool = False) -> QueryResult:
        """
        Validate a BigQuery query and optionally execute it.

        Args:
            query (str): The SQL query to execute.
            dry_run (bool): If True, only validate the query without executing it.

        Returns:
            QueryResult: The result of the query execution, including metadata and data.
        """
        try:
            # Always run as dry_run first to validate
            dry_run_job = self.client.query(
                query,
                job_config=bigquery.QueryJobConfig(
                    dry_run=True,
                    use_query_cache=False,
                    maximum_bytes_billed=MAX_BYTES_BILLED,
                ),
            )

            # Get statement type and validate if read-only
            statement_type = dry_run_job.statement_type
            if statement_type not in ALLOWED_STATEMENTS:
                raise ValueError(
                    f"Statement type '{statement_type}' is not allowed. Allowed types are: {', '.join(ALLOWED_STATEMENTS)}"
                )

            # Get referenced tables and validate allowed datasets
            referenced_tables = dry_run_job.referenced_tables
            if ALLOWED_DATASETS is not None:
                for table in referenced_tables:
                    if table.dataset_id not in ALLOWED_DATASETS:
                        raise ValueError(
                            f"Referenced table {table.project}.{table.dataset_id}.{table.table_id} is not allowed."
                        )

            # If dry_run=True, return the dry run job result
            if dry_run is True:
                return QueryResult(
                    rows=[],
                    total_rows=0,
                    schemas=[],
                    bytes_processed=dry_run_job.total_bytes_processed,
                    gbytes_processed=dry_run_job.total_bytes_processed / (1024 * 1024 * 1024),
                    job_id=dry_run_job.job_id,
                    referenced_tables=[f"{t.project}.{t.dataset_id}.{t.table_id}" for t in referenced_tables],
                    statement_type=statement_type,
                )

            # If dry_run=False, run the actual query
            query_job = self.client.query(
                query,
                job_config=bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED),
            )

            # Wait for the query to complete
            results = query_job.result()

            # Extract schema information
            schemas = []
            for field in results.schema:
                schemas.append(
                    TableSchema(name=field.name, type=field.field_type, mode=field.mode, description=field.description)
                )

            # Convert results to dicts
            rows = [dict(row.items()) for row in results]

            # Return formatted results
            return QueryResult(
                rows=rows,
                total_rows=len(rows),
                schemas=schemas,
                bytes_processed=query_job.total_bytes_processed,
                gbytes_processed=dry_run_job.total_bytes_processed / (1024 * 1024 * 1024),
                job_id=query_job.job_id,
                referenced_tables=[f"{t.project}.{t.dataset_id}.{t.table_id}" for t in referenced_tables],
                statement_type=statement_type,
            )

        except Exception as e:
            raise ValueError(f"Error executing query: {str(e)}") from e
