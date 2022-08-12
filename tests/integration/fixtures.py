from unittest.mock import Mock

FIXTURE_STATE_ID = "abc123"
FIXTURE_SERIALIZE_PATH = "/path/to/manifest"
FIXTURE_SOURCE_CODE = "select {{ 1 + 1 }} as id"
FIXTURE_COMPILED_CODE = "select 2 as id"


def get_state_mock(
    state_id=FIXTURE_STATE_ID,
    serialize_path=FIXTURE_SERIALIZE_PATH,
    compiled_code=FIXTURE_COMPILED_CODE,
    exception=None,
):
    state_mock = Mock()
    state_mock.state_id = state_id
    state_mock.serialize_path = serialize_path

    def compile_query(sql):
        if exception:
            raise exception

        return {"compiled_code": compiled_code}

    state_mock.compile_query = compile_query
    return state_mock
