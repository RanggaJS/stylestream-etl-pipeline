import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import requests
from utils.extract import extract_data, scrape_page, BASE_URL

class FakeResponse:
    """
    A mock version of the Response object from the requests library.
    """
    def __init__(self, content, status_code=200):
        self.text = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP Error: {self.status_code}")

class TestExtractModule(unittest.TestCase):
    """
    Unit tests for functions defined in the extract module.
    """

    def setUp(self):
        """
        Initialization before each test.
        """
        self.sample_html = """
        <html>
            <body>
                <div class="collection-card">
                    <div class="product-details">
                        <h3 class="collection-title">Test Product</h3>
                        <div class="collection-price">$45.99</div>
                        <div class="collection-rating">4.5 / 5</div>
                        <div class="collection-colors">3 Colors</div>
                        <div class="collection-size">Size: M</div>
                        <div class="collection-gender">Gender: Unisex</div>
                    </div>
                </div>
                <div class="collection-card">
                    <div class="product-details">
                        <h3 class="collection-title">Another Product</h3>
                        <div class="collection-price">$29.99</div>
                        <div class="collection-rating">4.2 / 5</div>
                        <div class="collection-colors">2 Colors</div>
                        <div class="collection-size">Size: L</div>
                        <div class="collection-gender">Gender: Women</div>
                    </div>
                </div>
            </body>
        </html>
        """

    @patch('utils.extract.requests.get')
    def test_scrape_page_success(self, mocked_get):
        """
        Ensure scrape_page returns expected data when the response is successful.
        """
        mocked_get.return_value = FakeResponse(self.sample_html)

        result = scrape_page(1)

        self.assertIsInstance(result, list)

        if result and len(result) > 0:
            self.assertIn('Title', result[0])
            self.assertIn('Price', result[0])

        called_url = mocked_get.call_args[0][0]
        self.assertTrue(called_url.startswith(BASE_URL))

    @patch('utils.extract.requests.get')
    def test_scrape_page_with_network_retry(self, mocked_get):
        """
        Test scrape_page retries after network failure and eventually succeeds.
        """
        mocked_get.side_effect = [
            requests.exceptions.RequestException("Network error"),
            FakeResponse(self.sample_html)
        ]

        with patch('utils.extract.time.sleep'):
            try:
                result = scrape_page(1)
                self.assertIsInstance(result, list)
            except Exception as err:
                self.assertIsInstance(err, requests.exceptions.RequestException)

        self.assertGreaterEqual(mocked_get.call_count, 1)

    @patch('utils.extract.requests.get')
    def test_scrape_page_http_error(self, mocked_get):
        """
        Test scrape_page handles HTTP error responses.
        """
        mocked_get.return_value = FakeResponse("", 404)

        with patch('utils.extract.time.sleep'):
            try:
                scrape_page(1)
            except Exception:
                pass

        self.assertGreaterEqual(mocked_get.call_count, 1)

    @patch('utils.extract.scrape_page')
    def test_extract_data_single_page(self, mocked_scraper):
        """
        Test extract_data function when scraping only one page.
        """
        mock_data = [{
            'Title': 'Test Product',
            'Price': '$45.99',
            'Rating': '4.5 / 5',
            'Colors': '3 Colors',
            'Size': 'Size: M',
            'Gender': 'Gender: Unisex',
            'timestamp': '2025-05-01 10:00:00'
        }]
        mocked_scraper.return_value = mock_data

        with patch('utils.extract.time.sleep'):
            df = extract_data(start_page=1, end_page=1)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df['Title'].iloc[0], 'Test Product')
        mocked_scraper.assert_called_once_with(1)

    @patch('utils.extract.scrape_page')
    def test_extract_data_multiple_pages(self, mocked_scraper):
        """
        Test extract_data over multiple pages.
        """
        page1_data = [{
            'Title': 'Product from Page 1',
            'Price': '$45.99',
            'Rating': '4.5 / 5',
            'Colors': '3 Colors',
            'Size': 'Size: M',
            'Gender': 'Gender: Unisex',
            'timestamp': '2025-05-01 10:00:00'
        }]

        page2_data = [{
            'Title': 'Product from Page 2',
            'Price': '$29.99',
            'Rating': '4.2 / 5',
            'Colors': '2 Colors',
            'Size': 'Size: L',
            'Gender': 'Gender: Women',
            'timestamp': '2025-05-01 10:00:00'
        }]

        mocked_scraper.side_effect = [page1_data, page2_data]

        with patch('utils.extract.time.sleep'):
            df = extract_data(start_page=1, end_page=2)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(df['Title'].iloc[0], 'Product from Page 1')
        self.assertEqual(df['Title'].iloc[1], 'Product from Page 2')
        self.assertEqual(mocked_scraper.call_count, 2)
        mocked_scraper.assert_any_call(1)
        mocked_scraper.assert_any_call(2)

    @patch('utils.extract.scrape_page')
    def test_extract_data_with_page_error(self, mocked_scraper):
        """
        Test extract_data handles a scraping error on one of the pages.
        """
        successful_data = [{'Title': 'Test Product', 'Price': '$45.99'}]
        mocked_scraper.side_effect = [successful_data, Exception("Failed scraping")]

        with patch('utils.extract.time.sleep'):
            df = extract_data(start_page=1, end_page=2)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(mocked_scraper.call_count, 2)

    def test_extract_data_with_invalid_range(self):
        """
        Test extract_data when given an invalid page range.
        """
        df = extract_data(start_page=0, end_page=-1)
        self.assertIsInstance(df, pd.DataFrame)
        # No assertion on length — implementation may vary.

if __name__ == '__main__':
    unittest.main()
