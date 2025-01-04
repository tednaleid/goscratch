package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/urfave/cli/v2"
	_ "github.com/marcboeker/go-duckdb"
)

type DataSource interface {
	CreateTableSQL(tableName, source string) string
}

type CSVDataSource struct{}

func (c *CSVDataSource) CreateTableSQL(tableName, source string) string {
	return fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s')", tableName, source)
}

type ParquetDataSource struct{}

func (p *ParquetDataSource) CreateTableSQL(tableName, source string) string {
	return fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')", tableName, source)
}

type IcebergDataSource struct{}

func (i *IcebergDataSource) CreateTableSQL(tableName, source string) string {
	return fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_iceberg('%s')", tableName, source)
}

const defaultPageSize = 10

type QuackServer struct {
	db           *sql.DB
	validColumns map[string]bool
	columnNames  []string
	tableName    string
}

type Link struct {
	Href string `json:"href"`
}

type Links struct {
	Values       Link `json:"values"`      // Points to unique values endpoint
	Records      Link `json:"records"`     // Template for querying records by query params
	RecordsAlias Link `json:"recordsAlias"` // Template for querying records by path
}

type ColumnSummary struct {
	Name           string      `json:"name"`
	Type           string      `json:"type"`
	Min            interface{} `json:"min,omitempty"`
	Max            interface{} `json:"max,omitempty"`
	ApproxUnique   int         `json:"approx_unique,omitempty"`
	Avg            interface{} `json:"avg,omitempty"`
	Std            interface{} `json:"std,omitempty"`
	Q25            interface{} `json:"q25,omitempty"`
	Q50            interface{} `json:"q50,omitempty"`
	Q75            interface{} `json:"q75,omitempty"`
	Count          int         `json:"count"`
	NullPercentage float64     `json:"null_percentage"`
	Links          Links       `json:"_links"`
}

func getDataSource(source string) (DataSource, error) {
	ext := strings.ToLower(filepath.Ext(source))
	switch ext {
	case ".csv":
		return &CSVDataSource{}, nil
	case ".parquet":
		return &ParquetDataSource{}, nil
	case ".iceberg":
		return &IcebergDataSource{}, nil
	default:
		_, err := url.Parse(source)
		if err != nil {
			return nil, fmt.Errorf("unsupported file format: %s", ext)
		}
		return &CSVDataSource{}, nil
	}
}

func NewQuackServer(source string) (*QuackServer, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("opening DuckDB: %w", err)
	}

	dataSource, err := getDataSource(source)
	if err != nil {
		db.Close()
		return nil, err
	}

	tableName := "data"
	if err := createTable(db, tableName, source, dataSource); err != nil {
		db.Close()
		return nil, err
	}

	columns, err := getValidColumns(db, tableName)
	if err != nil {
		db.Close()
		return nil, err
	}

	validColumns := make(map[string]bool)
	for _, col := range columns {
		validColumns[col] = true
	}

	return &QuackServer{
		db:           db,
		validColumns: validColumns,
		columnNames:  columns,
		tableName:    tableName,
	}, nil
}

func (s *QuackServer) Close() error {
	return s.db.Close()
}

func createTable(db *sql.DB, tableName, source string, ds DataSource) error {
	sql := ds.CreateTableSQL(tableName, source)
	_, err := db.Exec(sql)
	if err != nil {
		return fmt.Errorf("creating table from file: %w", err)
	}
	return nil
}

func getValidColumns(db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName))
	if err != nil {
		return nil, fmt.Errorf("getting column names: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("reading column names: %w", err)
	}
	return cols, nil
}

func (s *QuackServer) handleColumns(c echo.Context) error {
	query := fmt.Sprintf("SUMMARIZE SELECT * FROM %s", s.tableName)
	rows, err := s.db.Query(query)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	var summaries []ColumnSummary
	for rows.Next() {
		values := make([]interface{}, len(cols))
		for i := range values {
			values[i] = new(interface{})
		}

		if err := rows.Scan(values...); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}

		data := make(map[string]interface{})
		for i, col := range cols {
			if v := *(values[i].(*interface{})); v != nil {
				data[col] = v
			}
		}

		colName := data["column_name"].(string)
		summary := ColumnSummary{
			Name:           colName,
			Type:           data["column_type"].(string),
			Min:            data["min"],
			Max:            data["max"],
			ApproxUnique:   int(data["approx_unique"].(int64)),
			Count:          int(data["count"].(int64)),
			Links: Links{
				Values: Link{
					Href: fmt.Sprintf("/api/v1/columns/%s", colName),
				},
				Records: Link{
					Href: fmt.Sprintf("/api/v1/records?column=%s&value={value}", colName),
				},
				RecordsAlias: Link{
					Href: fmt.Sprintf("/api/v1/columns/%s/{value}", colName),
				},
			},
		}

		// Handle optional numeric fields
		if avg, ok := data["avg"]; ok && avg != nil {
			summary.Avg = avg
		}
		if std, ok := data["std"]; ok && std != nil {
			summary.Std = std
		}
		if q25, ok := data["q25"]; ok && q25 != nil {
			summary.Q25 = q25
		}
		if q50, ok := data["q50"]; ok && q50 != nil {
			summary.Q50 = q50
		}
		if q75, ok := data["q75"]; ok && q75 != nil {
			summary.Q75 = q75
		}

		// Convert null_percentage from decimal
		if np, ok := data["null_percentage"]; ok && np != nil {
			if str := fmt.Sprintf("%v", np); str != "" {
				if npFloat, err := strconv.ParseFloat(str, 64); err == nil {
					summary.NullPercentage = npFloat
				}
			}
		}

		summaries = append(summaries, summary)
	}

	return c.JSON(http.StatusOK, summaries)
}

func (s *QuackServer) handleQuery(c echo.Context) error {
	column := c.QueryParam("column")
	value := c.QueryParam("value")
	offset := 0
	limit := defaultPageSize

	if column == "" || value == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Both column and value parameters are required",
		})
	}

	if !s.validColumns[column] {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Invalid column name. Valid columns are: %s", strings.Join(s.columnNames, ", ")),
		})
	}

	if offsetStr := c.QueryParam("offset"); offsetStr != "" {
		var err error
		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Invalid offset parameter",
			})
		}
		if offset < 0 {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Offset must be non-negative",
			})
		}
	}

	if limitStr := c.QueryParam("limit"); limitStr != "" {
		var err error
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Invalid limit parameter",
			})
		}
		if limit < 1 {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Limit must be at least 1",
			})
		}
	}

	results, err := s.queryData(column, value, offset, limit)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, results)
}

func (s *QuackServer) handleColumnValues(c echo.Context) error {
	column := c.Param("column")

	if !s.validColumns[column] {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Invalid column name. Valid columns are: %s", strings.Join(s.columnNames, ", ")),
		})
	}

	query := fmt.Sprintf(`
		SELECT %[1]s as value, COUNT(*) as count 
		FROM %[2]s 
		GROUP BY %[1]s 
		ORDER BY count DESC, %[1]s
	`, column, s.tableName)

	rows, err := s.db.Query(query)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var value interface{}
		var count int64
		if err := rows.Scan(&value, &count); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		results = append(results, map[string]interface{}{
			"value": value,
			"count": count,
		})
	}

	return c.JSON(http.StatusOK, results)
}

func (s *QuackServer) handleColumnValueRedirect(c echo.Context) error {
	column := c.Param("column")
	value := c.Param("value")

	if !s.validColumns[column] {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Invalid column name. Valid columns are: %s", strings.Join(s.columnNames, ", ")),
		})
	}

	return c.Redirect(http.StatusFound, fmt.Sprintf("/api/v1/records?column=%s&value=%s&limit=%d&offset=0", 
		column, value, defaultPageSize))
}

func (s *QuackServer) queryData(column, value string, offset, limit int) ([]map[string]interface{}, error) {
	// Note: No explicit ORDER BY clause. Initial testing shows DuckDB maintains a stable
	// sort order for static files, but this is not guaranteed by the documentation.
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ? LIMIT ? OFFSET ?", 
		s.tableName, column)
	
	rows, err := s.db.Query(query, value, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("querying data: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		result, err := scanRow(rows, s.columnNames)
		if err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		results = append(results, result)
	}

	return results, nil
}

func scanRow(rows *sql.Rows, cols []string) (map[string]interface{}, error) {
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	result := make(map[string]interface{})
	for i, col := range cols {
		val := valuePtrs[i].(*interface{})
		result[col] = *val
	}

	return result, nil
}

func main() {
	app := &cli.App{
		Name:    "quack",
		Usage:   "A DuckDB-powered REST API server for querying data files",
		Version: "0.1.0",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "source",
				Aliases:  []string{"s"},
				Usage:    "Source file to load (CSV, Parquet, or Iceberg)",
				Required: true,
				EnvVars:  []string{"QUACK_SOURCE"},
			},
			&cli.IntFlag{
				Name:    "port",
				Aliases: []string{"p"},
				Usage:   "Port to run HTTP server on",
				Value:   9090,
				EnvVars: []string{"QUACK_PORT"},
			},
			&cli.IntFlag{
				Name:    "page-size",
				Aliases: []string{"n"},
				Usage:   "Default number of records per page",
				Value:   defaultPageSize,
				EnvVars: []string{"QUACK_PAGE_SIZE"},
			},
		},
		Action: func(c *cli.Context) error {
			server, err := NewQuackServer(c.String("source"))
			if err != nil {
				return fmt.Errorf("error initializing server: %w", err)
			}
			defer server.Close()

			e := echo.New()
			v1 := e.Group("/api/v1")
			v1.GET("/columns", server.handleColumns)
			v1.GET("/columns/:column", server.handleColumnValues)
			v1.GET("/columns/:column/:value", server.handleColumnValueRedirect)
			v1.GET("/records", server.handleQuery)

			return e.Start(fmt.Sprintf(":%d", c.Int("port")))
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
