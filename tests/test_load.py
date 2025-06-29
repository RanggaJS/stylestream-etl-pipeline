import unittest
import os
import pandas as pd
from unittest.mock import patch, MagicMock
from utils.load import save_to_csv, save_to_gsheets, save_to_postgres
from sqlalchemy.exc import SQLAlchemyError

class LoadModuleTests(unittest.TestCase):
    """
    Unit tests for the load module functions
    """

    def setUp(self):
        """
        Prepare test environment and sample data
        """
        # Sample DataFrame to use in tests
        self.test_data = pd.DataFrame({
            'Title': ['Test Product', 'Another Product'],
            'Price': [735840.0, 479840.0],  # Prices in IDR (45.99*16000, 29.99*16000)
            'Rating': [4.5, 4.2],
            'Colors': [3, 2],
            'Size': ['M', 'L'],
            'Gender': ['Unisex', 'Women'],
            'timestamp': ['2025-05-01 10:00:00', '2025-05-01 10:00:00']
        })

        # Temporary CSV filename for testing
        self.csv_test_file = "test_products.csv"

    def tearDown(self):
        """
        Cleanup after each test case
        """
        # Remove test CSV file if it exists
        if os.path.isfile(self.csv_test_file):
            os.remove(self.csv_test_file)

    def test_save_to_csv_functionality(self):
        """
        Verify save_to_csv correctly saves DataFrame to CSV
        """
        saved_path = save_to_csv(self.test_data, self.csv_test_file)

        # Check file creation
        self.assertTrue(os.path.isfile(saved_path))

        # Load CSV and validate content
        df_loaded = pd.read_csv(saved_path)

        self.assertEqual(len(df_loaded), len(self.test_data))
        self.assertEqual(len(df_loaded.columns), len(self.test_data.columns))
        self.assertEqual(df_loaded['Title'].iloc[0], 'Test Product')
        self.assertEqual(df_loaded['Price'].iloc[0], 735840.0)

    def test_save_to_csv_with_empty_dataframe(self):
        """
        Ensure save_to_csv raises ValueError when given an empty DataFrame
        """
        empty_df = pd.DataFrame()
        with self.assertRaises(ValueError):
            save_to_csv(empty_df, self.csv_test_file)

    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.googleapiclient.discovery.build')
    def test_save_to_gsheets_function(self, mock_build_func, mock_creds_func):
        """
        Test save_to_gsheets with mocked Google Sheets API calls
        """
        mock_sheets_service = MagicMock()
        mock_drive_service = MagicMock()
        mock_build_func.side_effect = [mock_sheets_service, mock_drive_service]

        mock_response = {'spreadsheetId': 'mock_spreadsheet_id'}
        mock_sheets_service.spreadsheets().create().execute.return_value = mock_response

        mock_credentials_file = "mock_credentials.json"
        with patch('os.path.exists', return_value=True):
            url = save_to_gsheets(self.test_data, mock_credentials_file)

        self.assertEqual(url, "https://docs.google.com/spreadsheets/d/mock_spreadsheet_id/edit")

        mock_build_func.assert_any_call('sheets', 'v4', credentials=mock_creds_func.return_value)
        mock_build_func.assert_any_call('drive', 'v3', credentials=mock_creds_func.return_value)

        mock_sheets_service.spreadsheets().values().update.assert_called_once()

    @patch('utils.load.service_account.Credentials.from_service_account_file')
    def test_save_to_gsheets_missing_credentials_file(self, mock_creds_func):
        """
        Confirm save_to_gsheets raises FileNotFoundError if credentials file is missing
        """
        with patch('os.path.exists', return_value=False):
            with self.assertRaises(FileNotFoundError):
                save_to_gsheets(self.test_data, "missing_credentials.json")

    @patch('utils.load.create_engine')
    def test_save_to_postgres_success(self, mock_engine_creator):
        """
        Test save_to_postgres successfully saves DataFrame to PostgreSQL
        """
        mock_engine_instance = MagicMock()
        mock_engine_creator.return_value = mock_engine_instance

        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql_func:
            success = save_to_postgres(self.test_data)

        self.assertTrue(success)
        mock_to_sql_func.assert_called_once_with("fashion_products", mock_engine_instance, if_exists='replace', index=False)

    @patch('utils.load.create_engine')
    def test_save_to_postgres_handles_sqlalchemy_error(self, mock_engine_creator):
        """
        Ensure save_to_postgres raises SQLAlchemyError on DB connection failure
        """
        mock_engine_creator.side_effect = SQLAlchemyError("DB connection failed")

        with self.assertRaises(SQLAlchemyError):
            save_to_postgres(self.test_data)

    def test_save_to_postgres_empty_dataframe(self):
        """
        Verify save_to_postgres raises ValueError with empty DataFrame input
        """
        empty_df = pd.DataFrame()
        with self.assertRaises(ValueError):
            save_to_postgres(empty_df)

    @patch('utils.load.create_engine')
    def test_save_to_postgres_with_custom_config(self, mock_engine_creator):
        """
        Test save_to_postgres with user-defined database connection parameters
        """
        mock_engine_instance = MagicMock()
        mock_engine_creator.return_value = mock_engine_instance

        db_config = {
            'host': 'custom_host',
            'database': 'custom_db',
            'user': 'custom_user',
            'password': 'custom_pass',
            'port': '5433'
        }

        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql_func:
            success = save_to_postgres(self.test_data, db_config)

        self.assertTrue(success)

        expected_conn_str = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        mock_engine_creator.assert_called_once_with(expected_conn_str)

if __name__ == '__main__':
    unittest.main()
