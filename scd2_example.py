import datetime

import pyarrow as pa
import pyarrow.compute as pc
from azure.identity import AzureCliCredential
from deltalake import DeltaTable, write_deltalake

if __name__ == "__main__":
    # For more details see: https://medium.com/@jimmy-jensen/type-2-slowly-changing-dimension-in-pure-delta-rs-for-python-6d29dcad6d5e
    table_name = 'abfs[s]://<workspace>(or <workspace_id>)@onelake.dfs.fabric.microsoft.com/<item>.<itemtype>(or <artifact_id>)/<path>/<fileName>'

    credentials = AzureCliCredential()
    access_token = credentials.get_token("https://storage.azure.com/.default").token
    storage_options = {"bearer_token": access_token, "use_fabric_endpoint": "true"}

    data = pa.table(
        {
            "code": [1, 2, 3],
            "x": ["foo", "bar", "baz"],
            "y": [100, 200, 300],
            "is_current": [True] * 3,
            "valid_from": [datetime.datetime(1900, 1, 1, 0, 0, 0, 000000).strftime("%Y-%m-%dT%H:%M:%S.%f")] * 3,
            "valid_to": [datetime.datetime(9999, 12, 31, 23, 59, 59, 999999).strftime("%Y-%m-%dT%H:%M:%S.%f")] * 3,
        }
    )

    write_deltalake(table_name, data, storage_options=storage_options, mode="overwrite")

    target_table = DeltaTable(table_name, storage_options=storage_options)

    target_pyarrow_table = target_table.to_pyarrow_table(filters=[("is_current", "==", True)])

    updates_table = pa.table(
        {
            "code": [1, 2, 4],
            "x": ["qux", "bar", "quux"],
            "y": [101, 200, 200],
        }
    )

    open_records_table = updates_table.join(
        right_table=target_pyarrow_table, keys='code', join_type='inner', right_suffix='_target'
    ).filter((pc.field("x") != pc.field("x_target")) | (pc.field("y") != pc.field("y_target")))

    # Create a new column called merge_key and fill with values from code column
    updates_table = updates_table.append_column(field_='merge_key', column=updates_table['code'])

    # Create a new column called merge_key and fill with nulls
    open_records_table = open_records_table.append_column(
        field_='merge_key', column=pa.array(obj=[None] * open_records_table.num_rows, type=pa.int64())
    )

    # Select records which exists in updates_table
    # - just to drop the right columns added by the inner join
    open_records_table = open_records_table.select(updates_table.column_names)

    # Combine the tables into a single table
    tables_to_merge = pa.concat_tables([open_records_table, updates_table])

    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("'%Y-%m-%dT%H:%M:%S.%f'")

    (
        target_table.merge(
            source=tables_to_merge,
            predicate="target.code = source.merge_key and target.is_current = true",
            source_alias="source",
            target_alias="target",
        )
        # This handles new valid records
        .when_not_matched_insert(
            updates={
                "code": "source.code",
                "x": "source.x",
                "y": "source.y",
                "is_current": "true",
                "is_deleted": "false",
                "valid_from": timestamp,
                "valid_to": datetime.datetime(9999, 12, 31, 23, 59, 59, 999999).strftime("'%Y-%m-%dT%H:%M:%S.%f'"),
            },
            predicate="merge_key is null",
            # These are entirely new records
        )
        .when_not_matched_insert(
            updates={
                "code": "source.code",
                "x": "source.x",
                "y": "source.y",
                "is_current": "true",
                "is_deleted": "false",
                "valid_from": datetime.datetime(1900, 1, 1, 0, 0, 0, 0).strftime("'%Y-%m-%dT%H:%M:%S.%f'"),
                "valid_to": datetime.datetime(9999, 12, 31, 23, 59, 59, 999999).strftime("'%Y-%m-%dT%H:%M:%S.%f'"),
            },
            predicate="merge_key is not null",
            # And this handles closing records
        )
        .when_matched_update(
            updates={
                "is_current": "false",
                "valid_to": timestamp,
            },
            predicate="source.x != target.x or source.y != target.y",
        )
        .execute()
    )
