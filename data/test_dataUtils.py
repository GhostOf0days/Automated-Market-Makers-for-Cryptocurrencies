from unittest import TestCase
from dataUtils import readCredentials
from data import JSONPATH
class Test(TestCase):
    def test_read_credentials(self):
        apiKey = readCredentials(JSONPATH)
        assert isinstance(apiKey, str) == True