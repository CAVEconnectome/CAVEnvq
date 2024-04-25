from itertools import chain


def status_table_name(base_table, suffix="status"):
    return f"{base_table}_{suffix}"


def flatten_list(list_of_lists):
    return list(chain.from_iterable(list_of_lists))


class TaskValidationError(Exception):
    "Raised when tasks do not have all needed data"

    pass


def expand_metadata_columns(df, extra_cols):
    for col in extra_cols:
        df[col] = [row["metadata"].get(col) for _, row in df.iterrows()]
    return df


def filter_complete_but_unprocessed(df, is_complete_col="is_complete", processed_col="processed"):
    return df.query(f"{processed_col} == False and {is_complete_col}== True")
