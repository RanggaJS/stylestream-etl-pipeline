import unittest
import os
import pandas as pd
from unittest.mock import patch, MagicMock
from utils.load import save_to_csv, save_to_gsheets, save_to_postgres
from sqlalchemy.exc import SQLAlchemyError

class TestLoadFunctions(unittest.TestCase):
    """
    Unit tests targeting the load module's save functions
    """

    def setUp(self):
        """
        Initialize test setup and create example DataFrame
        """
        self.sample_df = pd.DataFrame({
            'Title': ['Test Product', 'Another Product'],
            'Price': [735840.0, 479840.0],  # Converted from USD prices to IDR
            'Rating': [4.5, 4.2],
            'Colors': [3, 2],
            'Size': ['M', 'L'],
            'Gender': ['Unisex', 'Women'],
            'timestamp': ['2025-05-01 10:00:00', '2025-05-01 10:00:00']
        })

        self.test_csv_path = "test_products.csv"

    def tearDown(self):
        """
        Remove test artifacts after each test execution
        """
        if os.path.exists(self.test_csv_path):
            os.remove(self.test_csv_path)

    def test_save_to_csv_creates_file(self):
        """
        Check that save_to_csv writes DataFrame correctly to CSV file
        """
        output_path = save_to_csv(self.sample_df, self.test_csv_path)
        self.assertTrue(os.path.isfile(output_path))

        loaded_df = pd.read_csv(output_path)
        self.assertEqual(len(loaded_df), len(self.sample_df))
        self.assertEqual(len(loaded_df.columns), len(self.sample_df.columns))
        self.assertEqual(loaded_df['Title'].iloc[0], 'Test Product')
        self.assertEqual(loaded_df['Price'].iloc[0], 735840.0)

    def test_save_to_csv_rejects_empty_dataframe(self):
        """
        Confirm that passing an empty DataFrame to save_to_csv raises ValueError
        """
        empty_df = pd.DataFrame()
        with self.assertRaises(ValueError):
            save_to_csv(empty_df, self.test_csv_path)

    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.googleapiclient.discovery.build')
    def test_save_to_gsheets_with_mock(self, mock_build, mock_creds):
        """
        Validate save_to_gsheets with mocked Google Sheets API services
        """
        mock_sheets_service = MagicMock()
        mock_drive_service = MagicMock()
        mock_build.side_effect = [mock_sheets_service, mock_drive_service]

        mock_sheets_service.spreadsheets().create().execute.return_value = {'spreadsheetId': 'mock_id'}

        fake_creds_file = "fake_creds.json"
        with patch('os.path.exists', return_value=True):
            sheet_url = save_to_gsheets(self.sample_df, fake_creds_file)

        expected_url = "https://docs.google.com/spreadsheets/d/mock_id/edit"
        self.assertEqual(sheet_url, expected_url)

        mock_build.assert_any_call('sheets', 'v4', credentials=mock_creds.return_value)
        mock_build.assert_any_call('drive', 'v3', credentials=mock_creds.return_value)

        mock_sheets_service.spreadsheets().values().update.assert_called_once()

    @patch('utils.load.service_account.Credentials.from_service_account_file')
    def test_save_to_gsheets_fails_missing_credentials(self, mock_creds):
        """
        Ensure save_to_gsheets raises FileNotFoundError when credentials file is absent
        """
        with patch('os.path.exists', return_value=False):
            with self.assertRaises(FileNotFoundError):
                save_to_gsheets(self.sample_df, "nonexistent_creds.json")

    @patch('utils.load.create_engine')
    def test_save_to_postgres_successful_save(self, mock_create_engine):
        """
        Test that save_to_postgres properly writes data to PostgreSQL database
        """
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            result = save_to_postgres(self.sample_df)

        self.assertTrue(result)
        mock_to_sql.assert_called_once_with("fashion_products", mock_engine, if_exists='replace', index=False)

    @patch('utils.load.create_engine')
    def test_save_to_postgres_raises_on_connection_error(self, mock_create_engine):
        """
        Verify save_to_postgres raises SQLAlchemyError on engine creation failure
        """
        mock_create_engine.side_effect = SQLAlchemyError("Database connection error")

        with self.assertRaises(SQLAlchemyError):
            save_to_postgres(self.sample_df)

    def test_save_to_postgres_rejects_empty_dataframe(self):
        """
        Confirm save_to_postgres raises ValueError when given an empty DataFrame
        """
        empty_df = pd.DataFrame()
        with self.assertRaises(ValueError):
            save_to_postgres(empty_df)

    @patch('utils.load.create_engine')
    def test_save_to_postgres_with_custom_db_config(self, mock_create_engine):
        """
        Test save_to_postgres with custom database connection settings provided
        """
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        custom_db_settings = {
            'host': 'custom_host',
            'database': 'custom_db',
            'user': 'custom_user',
            'password': 'custom_password',
            'port': '5433'
        }

        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            result = save_to_postgres(self.sample_df, custom_db_settings)

        self.assertTrue(result)

        expected_connection_url = f"postgresql://{custom_db_settings['user']}:{custom_db_settings['password']}@{custom_db_settings['host']}:{custom_db_settings['port']}/{custom_db_settings['database']}"
        mock_create_engine.assert_called_once_with(expected_connection_url)

if __name__ == '__main__':
    unittest.main()
