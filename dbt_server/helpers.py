from dbt_server.exceptions import InternalException


def extract_compiled_code_from_node(result_node_dict):
    # https://github.com/dbt-labs/dbt-core/issues/5558
    #
    # dbt versions < 1.3 use `compiled_sql`, but this has been changed
    # to `compiled_code` in dbt-core v1.3
    compiled_code = result_node_dict.get("compiled_code", None)
    if not compiled_code:
        compiled_code = result_node_dict.get("compiled_sql", None)

    if not compiled_code:
        msg = "Failed to find compiled_sql or compiled_code in compiled node result"
        raise InternalException(msg)

    return compiled_code
