import unittest


class TestLogic(unittest.TestCase):
    MESSAGE_FMT = "Input: {0!r} - The correct result is {1!r}, receive {2!r}"

    def _test_all(self, func, cases):
        for input_, expect in cases:
            output = func(input_)
            msg = self.MESSAGE_FMT.format(input_, expect, output)
            self.assertEqual(output, expect, msg)
