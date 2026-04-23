"""Unit tests for crisp.utils.singleton_wrapper."""

import unittest

from crisp.utils.singleton_wrapper import SingletonWrapper


@SingletonWrapper
class _SampleClass:
    def __init__(self, value):
        self.value = value


class SingletonWrapperTests(unittest.TestCase):
    def tearDown(self):
        # Clear the singleton instances after each test so tests are
        # independent and ordering-insensitive.
        SingletonWrapper._instances.clear()

    def testSingletonInstance(self):
        instance1 = _SampleClass(10)
        instance2 = _SampleClass(20)

        self.assertIs(
            instance1,
            instance2,
            "SingletonWrapper did not return the same instance",
        )

        # The value should come from the first construction; the second
        # call's args must be discarded.
        self.assertEqual(
            instance1.value,
            10,
            "SingletonWrapper did not preserve the first initialized value",
        )
        self.assertEqual(
            instance2.value,
            10,
            "SingletonWrapper did not preserve the first initialized value",
        )

    def testDifferentClasses(self):
        @SingletonWrapper
        class _AnotherClass:
            def __init__(self, name):
                self.name = name

        sample_instance = _SampleClass(30)
        another_instance_1 = _AnotherClass("test1")
        another_instance_2 = _AnotherClass("test2")

        self.assertNotEqual(
            sample_instance,
            another_instance_1,
            "SingletonWrapper should not mix instances of different classes",
        )
        self.assertIs(
            another_instance_1,
            another_instance_2,
            "SingletonWrapper did not return the same instance for _AnotherClass",
        )
        self.assertEqual(
            another_instance_1.name,
            "test1",
            "SingletonWrapper did not preserve first value for _AnotherClass",
        )
        self.assertEqual(
            another_instance_2.name,
            "test1",
            "SingletonWrapper did not preserve first value for _AnotherClass",
        )

    def testSingletonAcrossMultipleCalls(self):
        first_call = _SampleClass(100)
        second_call = _SampleClass(200)

        self.assertIs(
            first_call,
            second_call,
            "SingletonWrapper did not return the same instance on multiple calls",
        )
        self.assertEqual(
            first_call.value,
            100,
            "SingletonWrapper should retain value of the first call",
        )


if __name__ == "__main__":
    unittest.main()
