from itertools import chain

import tomlkit


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


def make_config_for_tasklist(
    caveclient,
    nv_client,
    tasklist,
    extra_sieve_filters=None,
):
    if extra_sieve_filters is None:
        extra_sieve_filters = {}

    config = tomlkit.document()
    cave_config = tomlkit.table()
    cave_config.add("datastack_name", caveclient.datastack_name)
    cave_config.add("server_address", caveclient.server_address)
    config.add("cave", cave_config)

    nv_config = tomlkit.table()
    nv_config.add("url", nv_client.url)
    nv_config.add("namespace", tasklist.namespace)
    nv_config.add("extra_sieve", extra_sieve_filters)
    config.add("neuvue", nv_config)

    return tomlkit.dumps(config)


def parse_config(
    config_path,
):
    with open(config_path) as f:
        settings = tomlkit.parse(f.read())
    return settings
