from unittest.mock import MagicMock, patch
import subprocess
import unittest

# Test case to mock cqlsh execution
class TestInitCQL(unittest.TestCase):

    @patch("subprocess.run")
    def test_init_cql(self, mock_subprocess):
        # Simulate successful execution of cqlsh
        mock_subprocess.return_value = MagicMock(returncode=0)

        # Run the cqlsh command with init.cql
        subprocess.run(["cqlsh", "-f", "init.cql"])

        # Assert cqlsh was called with the correct arguments
        mock_subprocess.assert_called_with(["cqlsh", "-f", "init.cql"])

# Run the test
if __name__ == "__main__":
    unittest.main()

