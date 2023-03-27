from dbt_server.flags import EnvironFlag
from dbt_server.flags import InMemoryFlag
from os import environ
from unittest import TestCase

TEST_ENV_EXIST = "test_env"
TEST_ENV_NON_EXIST = "test_none"
TEST_VALUE = "test_value"
TEST_NEW_VALUE = "test_new_value"


class TestEnvironFlag(TestCase):
    def setUp(self) -> None:
        environ[TEST_ENV_EXIST] = TEST_VALUE
        if TEST_ENV_NON_EXIST in environ:
            del environ[TEST_ENV_NON_EXIST]

    def _get_flag_exist(self) -> None:
        return EnvironFlag(TEST_ENV_EXIST)

    def _get_flag_non_exist(self) -> None:
        return EnvironFlag(TEST_ENV_NON_EXIST)

    def test_get(self):
        self.assertEqual(self._get_flag_exist().get(), TEST_VALUE)
        self.assertIsNone(self._get_flag_non_exist().get())

    def test_set(self):
        flag = self._get_flag_exist()
        flag.set(TEST_NEW_VALUE)
        self.assertEqual(flag.get(), TEST_NEW_VALUE)
        self.assertEqual(environ[TEST_ENV_EXIST], TEST_NEW_VALUE)

        non_exist_flag = self._get_flag_non_exist()
        non_exist_flag.set(TEST_NEW_VALUE)
        self.assertEqual(non_exist_flag.get(), TEST_NEW_VALUE)
        self.assertEqual(environ[TEST_ENV_NON_EXIST], TEST_NEW_VALUE)

    def test_clear(self):
        flag = self._get_flag_exist()
        flag.clear()
        self.assertIsNone(flag.get())
        self.assertNotIn(TEST_ENV_EXIST, environ)

        non_exist_flag = self._get_flag_non_exist()
        non_exist_flag.clear()
        self.assertIsNone(non_exist_flag.get())
        self.assertNotIn(TEST_ENV_NON_EXIST, environ)


class TestInMemoryFlag(TestCase):
    def setUp(self) -> None:
        environ[TEST_ENV_EXIST] = TEST_VALUE
        if TEST_ENV_NON_EXIST in environ:
            del environ[TEST_ENV_NON_EXIST]

    def _get_flag_exist(self) -> None:
        return InMemoryFlag(TEST_ENV_EXIST, None)

    def _get_flag_non_exist(self) -> None:
        return InMemoryFlag(TEST_ENV_NON_EXIST, None)

    def test_get(self):
        flag = self._get_flag_exist()
        self.assertEqual(flag.get(), TEST_VALUE)

        non_exist_flag = self._get_flag_non_exist()
        self.assertIsNone(non_exist_flag.get())

    def test_set(self):
        flag = self._get_flag_exist()
        flag.set(TEST_NEW_VALUE)
        self.assertEqual(flag.get(), TEST_NEW_VALUE)
        self.assertEqual(environ[TEST_ENV_EXIST], TEST_VALUE)

        non_exist_flag = self._get_flag_non_exist()
        non_exist_flag.set(TEST_NEW_VALUE)
        self.assertEqual(flag.get(), TEST_NEW_VALUE)

    def test_clear(self):
        flag = self._get_flag_exist()
        flag.clear()
        self.assertIsNone(flag.get())
        self.assertIn(TEST_ENV_EXIST, environ)

        non_exist_flag = self._get_flag_non_exist()
        non_exist_flag.clear()
        self.assertIsNone(non_exist_flag.get())
        self.assertNotIn(TEST_ENV_NON_EXIST, environ)
