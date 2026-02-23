# Week 4 Lab: Building a Streamable HTTP MCP Server

Build an MCP (Model Context Protocol) server that exposes World Bank development data to AI agents like Claude.

## Learning Objectives

By completing this lab, you will:
- Understand the difference between MCP **Resources** (read-only data) and **Tools** (executable actions)
- Build an MCP server using **Streamable HTTP** transport
- Integrate local CSV data with external REST APIs
- Test your MCP server using the MCP Inspector and a Python client

## Overview

You will build an MCP server called `world-bank-server` that exposes:

| Part | Type | Description |
|------|------|-------------|
| **Part 1** | Resources | Read local World Bank indicator data from a CSV file |
| **Part 2** | Tools | Fetch live data from REST Countries and World Bank APIs |

When complete, an AI agent can ask questions like:
- "What's the GDP per capita of Germany?"
- "Compare the population of USA, China, and India"
- "Show me all indicators for Brazil from the local dataset"

## Why Resources vs Tools? (Design Pattern)

This lab intentionally uses **both** local data (Resources) and API calls (Tools) to teach an important MCP design pattern:

| | Resources (Local CSV) | Tools (API Calls) |
|---|---|---|
| **Data type** | Historical indicators (2000-2023) | Current country metadata |
| **Example** | "GDP of USA in 2015" | "What's the capital of USA?" |

**Why not just call the API every time?**

1. **Historical data doesn't change** - USA's GDP in 2015 is fixed forever. There's no reason to fetch it from an API repeatedly.

2. **Performance** - Local file reads are ~1ms. API calls are ~200-500ms. For bulk queries across 200 countries, that's the difference between instant and waiting 2 minutes.

3. **Reliability** - APIs can fail, have rate limits, or go down. Local data is always available.

4. **Cost at scale** - Many APIs charge per request or have rate limits. In production, you'd pay for every unnecessary API call.

5. **Offline capability** - Local resources work without internet. Your agent can still answer historical questions on an airplane.

6. **Data consistency** - You control the exact dataset version. APIs might update data or change formats unexpectedly.

**The real-world pattern:**
> Cache what you can (Resources), fetch what you must (Tools)

In production MCP servers, you typically:
- **Resources** for: configuration, cached data, historical records, local files, database snapshots
- **Tools** for: real-time data, actions with side effects, data that changes frequently, external service integrations

This is why we split the lab this way - it mirrors how you'd actually build a production MCP server.

## Prerequisites

- Python 3.11+
- `uv` package manager installed
- Basic understanding of async Python
- Familiarity with REST APIs

## Project Structure

```
lab-week4-mcp-server/
├── data/
│   └── world_bank_indicators.csv    # PROVIDED - World Bank data (do not modify)
├── server.py                         # STARTER CODE - implement the TODOs
├── test_client.py                    # PROVIDED - tests your server
├── pyproject.toml                    # PROVIDED - dependencies
├── .mcp.json                         # OPTIONAL - Claude Code config
└── README.md                         # YOU WRITE - document your implementation
```

## Setup

1. **Clone the repository** (via GitHub Classroom)

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Verify the data file exists**
   ```bash
   ls data/world_bank_indicators.csv
   ```

## Your Tasks

### Part 1: Implement Resources (25 points)

Resources expose **read-only data** from the local CSV file. Implement these three resources in `server.py`:

#### 1. `data://schema`
Return the column names and data types of the dataset.

```python
@mcp.resource("data://schema")
def get_schema() -> str:
    """Return the schema of the World Bank dataset."""
    # Already implemented as an example
```

#### 2. `data://countries`
Return a list of all unique countries in the dataset.

```python
@mcp.resource("data://countries")
def get_countries() -> str:
    """List all unique countries in the dataset."""
    df = _load_data()
    # TODO: Return unique country codes and names as JSON string
    # Hint: Use df.select() and df.unique() then df.write_json()
```

**Expected output format:**
```json
[
  {"countryiso3code": "USA", "country": {"value": "United States"}},
  {"countryiso3code": "CHN", "country": {"value": "China"}},
  ...
]
```

#### 3. `data://indicators/{country_code}`
Return all indicators for a specific country from the local data.

```python
@mcp.resource("data://indicators/{country_code}")
def get_country_indicators(country_code: str) -> str:
    """Get all indicators for a specific country from local data."""
    df = _load_data()
    # TODO: Filter by country_code and return as JSON string
    # Hint: Use df.filter() with pl.col("countryiso3code") == country_code
```

---

### Part 2: Implement Tools (30 points)

Tools are **executable functions** that call external APIs. Implement these three tools:

#### 1. `get_country_info(country_code: str)`
Fetch country metadata from the REST Countries API.

```python
@mcp.tool()
def get_country_info(country_code: str) -> dict:
    """Fetch detailed information about a country from REST Countries API."""
    logger.info(f"Fetching country info for: {country_code}")
    # TODO: Use _fetch_rest_countries() and extract relevant fields
    # Return: name, capital, region, subregion, languages, currencies, population, flag
```

**API Endpoint:** `https://restcountries.com/v3.1/alpha/{country_code}`

**Expected output:**
```python
{
    "name": "United States of America",
    "capital": "Washington, D.C.",
    "region": "Americas",
    "subregion": "North America",
    "languages": ["English"],
    "currencies": ["USD"],
    "population": 331002651,
    "flag": "🇺🇸"
}
```

#### 2. `get_live_indicator(country_code: str, indicator: str, year: int)`
Fetch a specific indicator value from the World Bank API.

```python
@mcp.tool()
def get_live_indicator(country_code: str, indicator: str, year: int = 2022) -> dict:
    """Fetch a specific indicator value from the World Bank API."""
    logger.info(f"Fetching {indicator} for {country_code} in {year}")
    # TODO: Use _fetch_world_bank_indicator() and return the value
```

**API Endpoint:** `https://api.worldbank.org/v2/country/{code}/indicator/{indicator}?format=json`

**Common Indicators:**
| Indicator ID | Description |
|--------------|-------------|
| `NY.GDP.PCAP.CD` | GDP per capita (current US$) |
| `SP.POP.TOTL` | Total population |
| `SP.DYN.LE00.IN` | Life expectancy at birth |
| `SE.ADT.LITR.ZS` | Adult literacy rate |

#### 3. `compare_countries(country_codes: list[str], indicator: str)`
Compare an indicator across multiple countries.

```python
@mcp.tool()
def compare_countries(country_codes: list[str], indicator: str, year: int = 2022) -> list[dict]:
    """Compare an indicator across multiple countries."""
    logger.info(f"Comparing {indicator} for countries: {country_codes}")
    # TODO: Call get_live_indicator for each country and collect results
```

**Expected output:**
```python
[
    {"country": "USA", "indicator": "SP.POP.TOTL", "value": 331002651, "year": 2022},
    {"country": "CHN", "indicator": "SP.POP.TOTL", "value": 1412175000, "year": 2022},
    {"country": "DEU", "indicator": "SP.POP.TOTL", "value": 83797985, "year": 2022}
]
```

---

### Part 3: Error Handling (15 points)

Your implementation should handle these error cases gracefully:

1. **Invalid country code** - Return a clear error message
2. **API request failure** - Catch exceptions and return error info
3. **Missing data** - Handle cases where indicator data doesn't exist for a year
4. **Empty results** - Handle when filters return no data

Example error handling:
```python
try:
    data = _fetch_rest_countries(country_code)
except httpx.HTTPStatusError as e:
    logger.error(f"API error for {country_code}: {e}")
    return {"error": f"Country not found: {country_code}"}
```

---

### Part 4: Test Your Server (15 points)

Demonstrate that your server works by testing it with **one of the following options**:

#### Option A: Python Test Client (Recommended)

Run the provided test client and capture the output to a log file:

```bash
# Terminal 1: Start your server
uv run python server.py

# Terminal 2: Run tests and save output to log file
uv run python test_client.py 2>&1 | tee test_results.log
```

**Requirements:**
- Include `test_results.log` in your repository
- The log should show all tests passing
- If any tests fail, fix your implementation before submitting

#### Option B: MCP Inspector

Test your server using the MCP Inspector visual UI:

```bash
# Terminal 1: Start your server
uv run python server.py

# Terminal 2: Start MCP Inspector
npx @modelcontextprotocol/inspector
```

**Requirements:**
- Take screenshots showing:
  1. Successfully connected to your server
  2. Testing at least one resource (show the response)
  3. Testing at least one tool (show the response)
- Create a `screenshots/` folder and include your screenshots
- Name them descriptively: `inspector-connected.png`, `inspector-resource-test.png`, `inspector-tool-test.png`

---

## Testing Your Server (Detailed Instructions)

### Option 1: MCP Inspector (Recommended)

The MCP Inspector provides a visual UI to test your server.

**Terminal 1 - Start your server:**
```bash
uv run python server.py
```

You should see:
```
Starting World Bank MCP Server on http://127.0.0.1:8765/mcp
Press Ctrl+C to stop
```

**Terminal 2 - Start MCP Inspector:**
```bash
npx @modelcontextprotocol/inspector
```

**In the Inspector UI:**
1. Select **"Streamable HTTP"** transport
2. Enter URL: `http://127.0.0.1:8765/mcp`
3. Click **Connect**
4. Navigate to **Resources** tab - test each resource
5. Navigate to **Tools** tab - test each tool

#### Running on EC2?

If you're running on an EC2 instance, use SSH tunneling:

```bash
# From your LOCAL machine (not EC2), run:
ssh -L 8765:localhost:8765 -L 6274:localhost:6274 ubuntu@your-ec2-ip

# Then open in your local browser:
# Inspector: http://localhost:6274
# Your server: http://localhost:8765/mcp
```

---

### Option 2: Python Test Client

We provide a test client that automatically tests all resources and tools.

**Terminal 1 - Start your server:**
```bash
uv run python server.py
```

**Terminal 2 - Run the test client:**
```bash
uv run python test_client.py
```

**Expected output:**
```
============================================================
TESTING RESOURCES
============================================================
Available resources: ['data://schema', 'data://countries', 'data://indicators/{country_code}']

Testing data://schema...
Schema: {'countryiso3code': 'String', 'country': 'String', ...}

Testing data://countries...
Countries: [{"countryiso3code": "USA", ...}]

Testing data://indicators/USA...
USA Indicators: [{"indicator": "NY.GDP.PCAP.CD", ...}]

============================================================
TESTING TOOLS
============================================================
Available tools: ['get_country_info', 'get_live_indicator', 'compare_countries']

Testing get_country_info('USA')...
Result: {"name": "United States of America", "capital": "Washington, D.C.", ...}

Testing get_live_indicator('USA', 'NY.GDP.PCAP.CD', 2022)...
Result: {"country": "USA", "indicator": "NY.GDP.PCAP.CD", "value": 76329.58, "year": 2022}

Testing compare_countries(['USA', 'CHN', 'DEU'], 'SP.POP.TOTL')...
Result: [{"country": "USA", ...}, {"country": "CHN", ...}, {"country": "DEU", ...}]

============================================================
ALL TESTS PASSED
============================================================
```

---

### Option 3: Claude Code Integration (Optional)

Once your server works, connect it to Claude Code:

**Create `.mcp.json` in your project root:**
```json
{
  "mcpServers": {
    "world-bank": {
      "type": "streamable-http",
      "url": "http://127.0.0.1:8765/mcp"
    }
  }
}
```

**Start your server, then restart Claude Code.**

Try these prompts:
- "What's the capital and population of Japan?"
- "Get the GDP per capita of Germany for 2022"
- "Compare life expectancy of USA, Germany, and Japan"

---

## API Reference

### REST Countries API (No API Key Required)

**Endpoint:** `https://restcountries.com/v3.1/alpha/{code}`

**Example:** `https://restcountries.com/v3.1/alpha/USA`

**Response fields you need:**
- `name.common` - Country name
- `capital[0]` - Capital city
- `region` - Geographic region
- `subregion` - Geographic subregion
- `languages` - Dictionary of languages
- `currencies` - Dictionary of currencies
- `population` - Population count
- `flag` - Flag emoji

---

### World Bank API (No API Key Required)

**Endpoint:** `https://api.worldbank.org/v2/country/{code}/indicator/{indicator}?format=json`

**Example:** `https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.PCAP.CD?format=json&date=2022`

**Response structure:**
```json
[
  { "page": 1, "pages": 1, "total": 1 },
  [
    {
      "indicator": {"id": "NY.GDP.PCAP.CD", "value": "GDP per capita"},
      "country": {"id": "US", "value": "United States"},
      "date": "2022",
      "value": 76329.58
    }
  ]
]
```

**Note:** Data is in the second element of the array (`response[1]`).

---

## Grading Rubric

| Component | Points | Criteria |
|-----------|--------|----------|
| **Resources (Part 1)** | 25 | All 3 resources implemented and return correct data |
| **Tools (Part 2)** | 30 | All 3 tools implemented, API calls work correctly |
| **Error Handling (Part 3)** | 15 | Graceful handling of invalid inputs, API failures |
| **Code Quality** | 15 | Type hints, docstrings, follows course coding standards |
| **Testing (Part 4)** | 15 | Test log file OR screenshots showing server works |

**Total: 100 points**

### Bonus Points (+10)

Integrate your MCP server with an AI assistant and provide proof:

**Option 1: Claude Code**
- Add your server to Claude Code via `.mcp.json`
- Take screenshots showing Claude using your tools
- Include screenshots in `screenshots/` folder

**Option 2: Other AI Assistants**
- Integrate with Goose, Cline, or another MCP-compatible assistant
- Take screenshots showing the assistant using your tools
- Include screenshots in `screenshots/` folder

**Screenshot requirements for bonus:**
- Show the AI assistant connected to your server
- Show it successfully calling at least one tool (e.g., "What's the GDP of Germany?")
- Show the response from your server

---

## Submission Checklist

Before submitting, verify:

- [ ] Server starts without errors: `uv run python server.py`
- [ ] All 3 resources return valid data
- [ ] All 3 tools call external APIs successfully
- [ ] Code follows course standards: `uv run ruff check .`
- [ ] **Testing evidence included:**
  - [ ] Option A: `test_results.log` file with passing tests, OR
  - [ ] Option B: `screenshots/` folder with MCP Inspector screenshots
- [ ] All changes committed and pushed to GitHub
- [ ] **(Bonus)** Screenshots of AI assistant using your server

---

## Troubleshooting

### "Connection refused" error
- Ensure your server is running (`uv run python server.py`)
- Check you're using `127.0.0.1` not `0.0.0.0`
- Verify port 8765 is not in use: `lsof -i :8765`

### "Module not found" error
- Run `uv sync` to install dependencies
- Ensure you're in the project directory

### API returns empty data
- World Bank API may not have data for all years
- Try a different year (2020, 2021, 2022)
- Check the country code is valid (use ISO 3166-1 alpha-3)

### Polars DataFrame issues
- Convert to JSON: `df.write_json()` returns a string
- Convert to dicts: `df.to_dicts()` returns a list of dictionaries
- Filter example: `df.filter(pl.col("column") == value)`

---

## Resources

- [MCP Documentation](https://modelcontextprotocol.io/)
- [FastMCP GitHub](https://github.com/jlowin/fastmcp) - Python SDK for building MCP servers
- [FastMCP Guide](https://gofastmcp.com/)
- [World Bank API Documentation](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-about-the-indicators-api-documentation)
- [REST Countries API](https://restcountries.com/)
- [Polars Documentation](https://docs.pola.rs/)

## FastMCP Quick Reference

### Creating a Streamable HTTP Server

```python
from mcp.server.fastmcp import FastMCP

# Initialize server with host and port
mcp = FastMCP(
    "my-server-name",
    host="127.0.0.1",
    port=8765,
)

# Define a resource (read-only data)
@mcp.resource("data://example")
def get_example_data() -> str:
    return "Hello from resource!"

# Define a tool (executable function)
@mcp.tool()
def my_tool(param: str) -> dict:
    return {"result": f"Processed: {param}"}

# Run with Streamable HTTP transport
if __name__ == "__main__":
    mcp.run(transport="streamable-http")
```

### Key Concepts

| Decorator | Purpose | Return Type |
|-----------|---------|-------------|
| `@mcp.resource("uri://path")` | Expose read-only data | `str` (usually JSON) |
| `@mcp.resource("uri://path/{param}")` | Parameterized resource | `str` |
| `@mcp.tool()` | Expose callable function | `dict` or any JSON-serializable |

### Transport Options

- **Streamable HTTP** (recommended): `mcp.run(transport="streamable-http")`
- **stdio**: `mcp.run(transport="stdio")` - for subprocess-based clients

---

# Implementation Details

## MCP Server Overview

This project implements a Streamable HTTP MCP server (`world-bank-server`) that exposes World Bank development indicators to AI agents.

The server integrates:

- Local historical indicator data from a CSV file (Resources)
- Live country and indicator data from external APIs (Tools)

Transport: Streamable HTTP on `http://127.0.0.1:8765/mcp`

---

## Resources Implemented

### `data://schema`
Returns dataset column names and types from the local CSV file.

### `data://countries`
Returns all unique country codes and country names in the dataset.

Implementation:
- Load CSV with Polars
- Select `countryiso3code` and `country`
- Return unique values as JSON

### `data://indicators/{country_code}`
Returns all indicator records for a given country from local data.

Implementation:
- Filter Polars DataFrame by `countryiso3code`
- Return JSON array
- Returns error JSON if country not found

---

## Tools Implemented

### `get_country_info(country_code)`
Fetches country metadata from REST Countries API.

Returned fields:
- name
- capital
- region
- subregion
- languages
- currencies
- population
- flag

Error handling included for invalid codes and API failures.

---

### `get_live_indicator(country_code, indicator, year)`
Fetches a World Bank indicator value for a specific country and year.

Implementation:
- Calls World Bank API
- Matches requested year
- Returns indicator metadata and value
- Returns error if no data available

---

### `compare_countries(country_codes, indicator, year)`
Compares an indicator across multiple countries.

Implementation:
- Loops through country list
- Fetches indicator for each country
- Returns list of results
- Individual country failures do not stop execution

---

## Error Handling

The server gracefully handles:

- Invalid country codes
- API request failures
- Missing indicator data
- Empty dataset filters

All errors return structured JSON messages instead of crashing.

---

## How to Run

Start MCP server:

```bash
uv run python server.py
````

Server endpoint:

```
http://127.0.0.1:8765/mcp
```

---

## Test Results

All provided MCP tests pass successfully using the Python test client.

Run tests:

```bash
uv run python test_client.py
```

See `test_results.log` for full output showing:

```
ALL TESTS PASSED
```