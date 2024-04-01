from cavenvq.utils import status_table_name


def create_task_tables(
    table_name, description, voxel_resolution, cv_client, schema="bound_tag", reference_suffix="status"
):
    "Convenience function to generate a proofreading task table and its status reference table."

    ref_table = status_table_name(table_name, reference_suffix)
    ref_schema = "reference_tag"
    ref_description = f"Proofreading status tracking table for `{table_name}`."

    existing_tables = cv_client.annotation.get_tables()
    if table_name not in existing_tables and ref_table not in existing_tables:
        cv_client.annotation.create_table(
            table_name=table_name,
            schema_name=schema,
            description=description,
            voxel_resolution=voxel_resolution,
        )

        cv_client.annotation.create_table(
            table_name=ref_table,
            schema_name=ref_schema,
            description=ref_description,
            voxel_resolution=voxel_resolution,
            reference_table=table_name,
            track_target_id_updates=True,
        )

        print(f"Created tables: {table_name} and {ref_table}")
