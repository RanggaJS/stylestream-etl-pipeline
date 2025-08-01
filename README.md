# StyleStream ETL

## 📋 Overview

**StyleStream ETL** is a robust data pipeline built to extract, transform, and load (ETL) fashion product data from web sources into multiple storage systems. It streamlines the process of gathering and preparing fashion data for structured and automated analysis.

## 🌟 Features

- **Automated Data Extraction**: Scrapes product data from fashion websites
- **Data Transformation**: Cleans, formats, and standardizes raw data
- **Flexible Data Loading**: Supports CSV export, Google Sheets integration, and PostgreSQL storage
- **Comprehensive Logging**: Tracks the ETL pipeline process
- **Robust Error Handling**: Detects and manages common runtime issues

## 🛠️ Technologies Used

- **Python 3.8+**: Core programming language
- **BeautifulSoup**: Web scraping tool
- **Pandas**: Data manipulation and transformation
- **Google Sheets API**: For exporting data to Google Sheets
- **SQLAlchemy**: ORM for PostgreSQL integration
- **Pytest**: Automated testing framework

## 🚀 Installation

### Prerequisites

- Python 3.8 or later
- pip (Python package manager)
- Internet access to install dependencies

### Installation Steps

1. Clone the repository:

```bash
git clone https://github.com/habstrakT808/ETL---Fashion-Studio-Store.git
cd ETL-Fashion-Studio
```

2. (Optional but recommended) Set up a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # For Linux/Mac
# OR
venv\Scripts\activate  # For Windows
```

3. Install the required dependencies:

```bash
pip install -r requirements.txt
```

4. Set up Google Sheets API credentials (see below)

## 🔑 Setting Up Google Sheets API Credentials

To enable Google Sheets integration, follow these steps:

1. Create a Project on Google Cloud Console

- Visit Google Cloud Console
- Create a new project (e.g., "StyleStream ETL")
- Note the Project ID

2. Enable the Google Sheets API

- Navigate to "APIs & Services" > "Library"
- Search for "Google Sheets API" and enable it

3. Create a Service Account

- Go to "APIs & Services" > "Credentials"
- Click "Create Credentials" > "Service Account"
- Fill in the required details
- Assign the Editor role
- Click "Done"

4. Generate a Key

- Click on the newly created service account
- Open the "Keys" tab
- Click "Add Key" > "Create new key"
- Choose the JSON format
- Click "Create" (this will download a .json file)

5. Save the Credentials File

- Rename the downloaded file to google-sheets-api.json
- Place it in the root directory of this project
- ⚠️ Do not commit this file (already added to .gitignore)

## 📊 Usage

### Run the ETL Pipeline

```bash
python main.py
```

### Run Unit Tests

```bash
python -m pytest tests
```

### Run Test Coverage

```bash
coverage run -m pytest tests
coverage report
```

## 📁 Project Structure

```bash
ETL-Fashion-Studio/
├── main.py                   # Entry point for the ETL pipeline
├── requirements.txt          # List of dependencies
├── .gitignore                # Files ignored by Git
├── README.md                 # Project documentation
├── products.csv              # Output data in CSV format
├── google-sheets-api.json    # Google API credentials (not committed)
├── utils/                    # Utility modules
│   ├── __init__.py
│   ├── extract.py            # Web scraping module
│   ├── transform.py          # Data transformation module
│   └── load.py               # Data loading module
└── tests/                    # Unit tests
    ├── __init__.py
    ├── test_extract.py
    ├── test_transform.py
    └── test_load.py
```

## 📈 Sample Output

### CSV Output

Google Sheets Output
Access the example spreadsheet here:
https://drive.google.com/file/d/1dIUkoIX7-XwwHBaY2PYaPulL0xR4TvI6/view

## 🤝 Contributing

Contributions are welcome! To contribute:

1. Fork this repository
2. Create a new branch (git checkout -b feature/your-feature)
3. Commit your changes (git commit -m "Add your feature")
4. Push to your branch (git push origin feature/your-feature)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License. See the LICENSE file for more information.

## 📞 Contact

Rangga – ranggajs235@gmail.com
⭐️ If you like this project, feel free to give it a star!
#
