# Quack

`quack` is a DuckDB-powered REST API server that allows you to query data files (CSV, Parquet, or Iceberg) through a simple HTTP interface.

## Usage

```
NAME:
   quack - A DuckDB-powered REST API server for querying data files

USAGE:
   quack [global options] command [command options]

VERSION:
   0.1.0

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --source value, -s value     Source file to load (CSV, Parquet, or Iceberg) [$QUACK_SOURCE]
   --port value, -p value       Port to run HTTP server on (default: 9090) [$QUACK_PORT]
   --page-size value, -n value  Default number of records per page (default: 10) [$QUACK_PAGE_SIZE]
   --help, -h                   show help
   --version, -v                print the version
```

## Running Locally

To run Quack locally using the provided `temperatures.csv` file:

```bash
# Run on default port 9090
go run cmd/quack/main.go -s cmd/quack/temperatures.csv

# Run on a custom port
go run cmd/quack/main.go -s cmd/quack/temperatures.csv -p 8080

# Run with custom page size
go run cmd/quack/main.go -s cmd/quack/temperatures.csv -n 20
```

## API Endpoints

The API is versioned and all endpoints are prefixed with `/api/v1`.

Examples use `jq` (`brew install jq`) to parse smaller, formatted responses.

### Get Column Information

```bash
# Get metadata for all columns
curl -sL "http://localhost:9090/api/v1/columns" | jq '.[0]'
{
  "name": "airport_code",
  "type": "VARCHAR",
  "min": "ATL",
  "max": "SFO",
  "approx_unique": 13,
  "count": 188,
  "null_percentage": 0,
  "_links": {
    "values": {
      "href": "/api/v1/columns/airport_code"
    },
    "records": {
      "href": "/api/v1/records?column=airport_code&value={value}"
    },
    "recordsAlias": {
      "href": "/api/v1/columns/airport_code/{value}"
    }
  }
}
```

### Get Column Values

```bash
# Get unique values and counts for a specific column
curl -sL "http://localhost:9090/api/v1/columns/airport_code" | jq '.[0:4]'
[
  {
    "count": 15,
    "value": "DFW"
  },
  {
    "count": 15,
    "value": "JFK"
  },
  {
    "count": 15,
    "value": "LAX"
  },
  {
    "count": 15,
    "value": "MSP"
  }
]
```

### Query Records

```bash
# Query records for a column value
curl -sL "http://localhost:9090/api/v1/columns/airport_code/MSP" | jq '.[0:2]'
[
  {
    "airport_code": "MSP",
    "celsius": -15.5,
    "date": "2025-01-01T00:00:00Z"
  },
  {
    "airport_code": "MSP",
    "celsius": -13.3,
    "date": "2025-01-02T00:00:00Z"
  }
]

it will redirect you to a `/records` path that lets you set the `limit` and `offset` parameters for pagination:

```bash
curl -sL "http://localhost:9090/api/v1/records?column=airport_code&value=MSP&limit=3&offset=6" | jq '.'
[
  {
    "airport_code": "MSP",
    "celsius": -12.2,
    "date": "2025-01-07T00:00:00Z"
  },
  {
    "airport_code": "MSP",
    "celsius": -11.1,
    "date": "2025-01-08T00:00:00Z"
  },
  {
    "airport_code": "MSP",
    "celsius": -12.7,
    "date": "2025-01-09T00:00:00Z"
  }
]
```