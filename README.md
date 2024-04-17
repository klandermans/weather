# DWD Weather Forecast Parser

This Python script parses weather forecast data from the Deutscher Wetterdienst (DWD) and saves it into a database. It retrieves forecast data for various stations and stores it in a structured format for further analysis or use in applications.

## Features

- Retrieves weather forecast data from DWD for specified stations and forecast dates.
- Parses the forecast data and stores it in a structured format (DataFrame).
- Cleans the data and handles missing values.
- Can be extended to save the data into a database for long-term storage.

## Dependencies

- Python 3
- pandas
- numpy
- urllib
- zipfile
- xml.etree.ElementTree

## Installation

1. Clone this repository to your local machine:

```
git clone <repository-url>
```

2. Install the required dependencies using pip:

```
pip install pandas numpy urllib zipfile
```

## Usage

1. Run the `parse.py` script to start parsing weather forecast data:

```
python parse.py
```

2. The script will automatically retrieve forecast data for the current date and specified stations, parse it, and store it in a structured format.

## Configuration

- The script retrieves station codes and forecast data URLs from DWD's open data portal.
- You can modify the `Parse` class to customize parsing logic or add additional functionality.

## Contributors

- [Your Name](https://github.com/your-username)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
