from unittest import TestCase, mock
import asyncio
import configparser
import sofiax
from sofiax.merge import run_merge


class TestCaseSuccessfulExecution(TestCase):
    """!Execute test case successfully.

    """
    def setUp(self):
        # Default values for test case
        self.conf = 'test_case/config.ini'
        self.param = ['test_case/sofia.par']
        self.run_name = 'Test'
        self.sanity = {
            'flux': 5,
            'spatial_extent': (5, 5),
            'spectral_extent': (5, 5),
            'uncertainty_sigma': 5
        }

        # creating arguments for run_merge 
        self.config = configparser.ConfigParser()
        self.config.read(self.conf)

    # @mock.patch('sofiax.merge.db_run_upsert')
    # @mock.patch('sofiax.merge.db_instance_upsert')
    @mock.patch('sofiax.merge.match_merge_detections')
    def test_sofiax_run_test_case(self, mock_merge):
        """Mock values for test case run.
        Note this assumes the test_case folder is in the
        subdirectory.

        """
        # run merge in event loop
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            run_merge(
                self.config,
                self.run_name,
                self.param,
                self.sanity
            )
        )

        # check mocks were called
        mock_merge.assert_called_once()

    @mock.patch('sofiax.merge.asyncio.create_subprocess_shell')
    def test_does_not_run_sofia(self, mock_subprocess):
        """Mock test case does not run sofia as a subprocess
        based on default value in config.

        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            run_merge(
                self.config,
                self.run_name,
                self.param,
                self.sanity
            )
        )

        mock_subprocess.assert_not_called()
