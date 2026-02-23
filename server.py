"""
Week 4 Lab: World Bank Data MCP Server

An MCP server that exposes:
- Resources: Local World Bank indicator data from CSV
- Tools: Live data from REST Countries and World Bank APIs

Transport: Streamable HTTP on port 8765
"""
import json
import logging
from pathlib import Path
from typing import Optional

import httpx
import polars as pl
from mcp.server.fastmcp import FastMCP


# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_FILE: Path = Path(__file__).parent / "data" / "world_bank_indicators.csv"
HOST: str = "127.0.0.1"
PORT: int = 8765

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)

# Initialize MCP server
mcp = FastMCP(
    "world-bank-server",
    host=HOST,
    port=PORT,
)


# =============================================================================
# PRIVATE HELPER FUNCTIONS
# =============================================================================

def _load_data() -> pl.DataFrame:
    """Load the World Bank indicators CSV file."""
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")
    return pl.read_csv(DATA_FILE)


# def _fetch_rest_countries(country_code: str) -> dict:
#     """Fetch country info from REST Countries API."""
#     url = f"https://restcountries.com/v3.1/alpha/{country_code}"
#     with httpx.Client(timeout=30.0) as client:
#         response = client.get(url)
#         response.raise_for_status()
#         return response.json()[0]

def _fetch_rest_countries(country_code: str) -> dict:
    """Fetch country info from REST Countries API."""
    url = f"https://restcountries.com/v3.1/alpha/{country_code}"
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        data = response.json()

        # REST Countries typically returns a list, taking the first item; however, it's important to prevent empty lists.
        if isinstance(data, list):
            if not data:
                raise ValueError(f"No country found for code: {country_code}")
            return data[0]

        # In rare cases, it might be dict
        if isinstance(data, dict):
            return data

        raise ValueError(f"Unexpected REST Countries response type: {type(data)}")


def _fetch_world_bank_indicator(
    country_code: str,
    indicator: str,
    year: Optional[int] = None,
) -> list:
    """Fetch indicator from World Bank API."""
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}"
    params = {"format": "json", "per_page": 100}
    if year:
        params["date"] = str(year)

    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if len(data) < 2 or not data[1]:
            return []
        return data[1]


# =============================================================================
# PART 1: RESOURCES (Local Data)
# =============================================================================

@mcp.resource("data://schema")
def get_schema() -> str:
    """
    Return the schema of the World Bank dataset.

    This resource is provided as an example - it's already implemented.
    """
    df = _load_data()
    schema_info = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
    return json.dumps(schema_info, indent=2)


@mcp.resource("data://countries")
def get_countries() -> str:
    """
    List all unique countries in the dataset.

    TODO: Implement this resource.

    Hints:
    - Use _load_data() to get the DataFrame
    - Use df.select() to pick relevant columns (countryiso3code, country)
    - Use df.unique() to get unique values
    - Return as JSON string using json.dumps() or df.write_json()

    Expected output format:
    [
        {"countryiso3code": "USA", "country": "United States"},
        {"countryiso3code": "CHN", "country": "China"},
        ...
    ]
    """
    df = _load_data()
    # TODO: Implement - return unique country codes and names as JSON string
    countries = (
        df.select(["countryiso3code", "country"])
        .unique()
        .sort("countryiso3code")
    )
    return countries.write_json()


@mcp.resource("data://indicators/{country_code}")
def get_country_indicators(country_code: str) -> str:
    """
    Get all indicators for a specific country from local data.

    TODO: Implement this resource.

    Args:
        country_code: ISO 3166-1 alpha-3 country code (e.g., "USA", "CHN", "DEU")

    Hints:
    - Use _load_data() to get the DataFrame
    - Use df.filter(pl.col("countryiso3code") == country_code) to filter
    - Return as JSON string
    - Handle case where country_code is not found (return error message)

    Expected output: JSON array of indicator records for that country
    """
    df = _load_data()
    # TODO: Implement - filter by country and return as JSON string
    result = df.filter(pl.col("countryiso3code") == country_code)

    if result.height == 0:
        return json.dumps({"error": f"No data found for country_code: {country_code}"})

    return result.write_json()


# =============================================================================
# PART 2: TOOLS (External APIs)
# =============================================================================

@mcp.tool()
def get_country_info(country_code: str) -> dict:
    """
    Fetch detailed information about a country from REST Countries API.

    TODO: Implement this tool.

    Args:
        country_code: ISO 3166-1 alpha-2 or alpha-3 country code (e.g., "US", "USA", "DE")

    Returns:
        Dictionary with country information including:
        - name: Common name of the country
        - capital: Capital city
        - region: Geographic region (e.g., "Americas", "Europe")
        - subregion: Geographic subregion
        - languages: List of official languages
        - currencies: List of currency codes
        - population: Current population
        - flag: Flag emoji

    Hints:
    - Use _fetch_rest_countries(country_code) to get raw API data
    - Extract the relevant fields from the response
    - Handle errors gracefully (invalid country code, API failure)

    API Response structure (key fields):
    - name.common: "United States"
    - capital: ["Washington, D.C."]
    - region: "Americas"
    - subregion: "North America"
    - languages: {"eng": "English"}
    - currencies: {"USD": {"name": "United States dollar", ...}}
    - population: 331002651
    - flag: "🇺🇸"
    """
    logger.info(f"Fetching country info for: {country_code}")
    # TODO: Implement using _fetch_rest_countries()
    try:
        raw = _fetch_rest_countries(country_code)

        name = raw.get("name", {}).get("common") or raw.get("name", {}).get("official")
        capital_list = raw.get("capital") or []
        capital = capital_list[0] if capital_list else None

        languages = list((raw.get("languages") or {}).values())
        currencies = list((raw.get("currencies") or {}).keys())

        return {
            "name": name,
            "capital": capital,
            "region": raw.get("region"),
            "subregion": raw.get("subregion"),
            "languages": languages,
            "currencies": currencies,
            "population": raw.get("population"),
            "flag": raw.get("flag"),
        }

    except httpx.HTTPStatusError as e:
        logger.error(f"REST Countries API error for {country_code}: {e}")
        return {"error": f"Country not found: {country_code}"}
    except Exception as e:
        logger.error(f"Error fetching country info for {country_code}: {e}")
        return {"error": f"Failed to fetch country info for {country_code}"}


@mcp.tool()
def get_live_indicator(
    country_code: str,
    indicator: str,
    year: int = 2022,
) -> dict:
    """
    Fetch a specific indicator value from the World Bank API.

    TODO: Implement this tool.

    Args:
        country_code: ISO 3166-1 alpha-2 or alpha-3 country code
        indicator: World Bank indicator ID (e.g., "NY.GDP.PCAP.CD" for GDP per capita)
        year: Year to fetch data for (default: 2022)

    Returns:
        Dictionary with:
        - country: Country code
        - country_name: Full country name
        - indicator: Indicator ID
        - indicator_name: Human-readable indicator name
        - year: Year of data
        - value: The indicator value

    Common indicators:
        - NY.GDP.PCAP.CD: GDP per capita (current US$)
        - SP.POP.TOTL: Total population
        - SP.DYN.LE00.IN: Life expectancy at birth
        - SE.ADT.LITR.ZS: Adult literacy rate

    Hints:
    - Use _fetch_world_bank_indicator(country_code, indicator, year)
    - The API returns a list; find the entry matching the requested year
    - Handle case where no data exists for that year
    """
    logger.info(f"Fetching {indicator} for {country_code} in {year}")
    # TODO: Implement using _fetch_world_bank_indicator()
    try:
        records = _fetch_world_bank_indicator(country_code, indicator, year)

        if not records:
            return {
                "country": country_code,
                "indicator": indicator,
                "year": year,
                "value": None,
                "error": f"No data found for {country_code} {indicator} in {year}",
            }

        match = None
        for r in records:
            if str(r.get("date")) == str(year):
                match = r
                break

        if match is None:
            match = records[0]

        return {
            "country": country_code,
            "country_name": (match.get("country") or {}).get("value"),
            "indicator": indicator,
            "indicator_name": (match.get("indicator") or {}).get("value"),
            "year": year,
            "value": match.get("value"),
        }

    except httpx.HTTPStatusError as e:
        logger.error(f"World Bank API error: {e}")
        return {"error": f"API error fetching {indicator} for {country_code} in {year}"}
    except Exception as e:
        logger.error(f"Error fetching indicator: {e}")
        return {"error": f"Failed to fetch indicator {indicator} for {country_code} in {year}"}


@mcp.tool()
def compare_countries(
    country_codes: list[str],
    indicator: str,
    year: int = 2022,
) -> list[dict]:
    """
    Compare an indicator across multiple countries.

    TODO: Implement this tool.

    Args:
        country_codes: List of ISO country codes to compare (e.g., ["USA", "CHN", "DEU"])
        indicator: World Bank indicator ID to compare
        year: Year to fetch data for

    Returns:
        List of dictionaries, one per country, each containing:
        - country: Country code
        - country_name: Full country name
        - indicator: Indicator ID
        - year: Year
        - value: The indicator value (or None if not available)

    Hints:
    - Loop through country_codes and call get_live_indicator() for each
    - Collect results into a list
    - Handle errors for individual countries (don't fail the whole request)
    """
    logger.info(f"Comparing {indicator} for countries: {country_codes}")
    # TODO: Implement - call get_live_indicator for each country
    results: list[dict] = []

    for code in country_codes:
        try:
            records = _fetch_world_bank_indicator(code, indicator, year)

            if not records:
                results.append({
                    "country": code,
                    "country_name": None,
                    "indicator": indicator,
                    "indicator_name": None,
                    "year": year,
                    "value": None,
                    "error": f"No data found for {code} {indicator} in {year}",
                })
                continue

            # Find matching year
            match = None
            for r in records:
                if str(r.get("date")) == str(year):
                    match = r
                    break
            if match is None:
                match = records[0]

            results.append({
                "country": code,
                "country_name": (match.get("country") or {}).get("value"),
                "indicator": indicator,
                "indicator_name": (match.get("indicator") or {}).get("value"),
                "year": year,
                "value": match.get("value"),
            })

        except httpx.HTTPStatusError as e:
            logger.error(f"World Bank API error for {code}: {e}")
            results.append({
                "country": code,
                "country_name": None,
                "indicator": indicator,
                "indicator_name": None,
                "year": year,
                "value": None,
                "error": "API request failed",
            })
        except Exception as e:
            logger.error(f"Error comparing {code}: {e}")
            results.append({
                "country": code,
                "country_name": None,
                "indicator": indicator,
                "indicator_name": None,
                "year": year,
                "value": None,
                "error": "Failed to fetch",
            })

    return results


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    logger.info(f"Starting World Bank MCP Server on http://{HOST}:{PORT}/mcp")
    logger.info(f"Connect with MCP Inspector or test client at http://{HOST}:{PORT}/mcp")
    logger.info("Press Ctrl+C to stop")
    mcp.run(transport="streamable-http")
