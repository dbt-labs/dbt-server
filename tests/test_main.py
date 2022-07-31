"""Test cases for the __main__ module."""
import asyncio

import pytest
from click.testing import CliRunner


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


def test_sanity() -> None:
    assert 5 == 5
    assert 4 != 3


def test_dependent_private_event_loop_values_exist() -> None:
    """Assert that some private values we depend on exist.

    We depend on a couple private fields defined in the asyncio
    package. This test gives us confidence that new versions of
    Python maintain those fields.
    """

    async def assert_event_loop_values():
        loop = asyncio.get_event_loop()
        assert hasattr(loop, "_signal_handlers")
        assert isinstance(getattr(loop, "_signal_handlers"), dict)

    asyncio.run(assert_event_loop_values())


def test_dependent_private_handle_values_exist() -> None:
    """Assert that some private values we depend on exist.

    We depend on a couple private fields defined in the asyncio
    package. This test gives us confidence that new versions of
    Python maintain those fields.
    """

    async def assert_asyncio_handle_values():
        # import this inline to prevent the test
        # module from blowing up on load, jic this
        # is removed in a future version of python
        from asyncio.events import Handle

        assert hasattr(Handle, "_run")
        assert callable(getattr(Handle, "_run"))

    asyncio.run(assert_asyncio_handle_values())
