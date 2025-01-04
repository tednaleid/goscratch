package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
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

type QuackServer struct {
	db           *sql.DB
	validColumns map[string]bool
	columnNames  []string
	tableName    string
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
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(cols))
		for i := range values {
			values[i] = new(interface{})
		}

		if err := rows.Scan(values...); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}

		// Create a map of the column data
		data := make(map[string]interface{})
		for i, col := range cols {
			if v := *(values[i].(*interface{})); v != nil {
				data[col] = v
			}
		}

		// Map the data to our struct
		summary := ColumnSummary{
			Name:           data["column_name"].(string),
			Type:           data["column_type"].(string),
			Min:            data["min"],
			Max:            data["max"],
			ApproxUnique:   int(data["approx_unique"].(int64)),
			Count:          int(data["count"].(int64)),
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
	column := c.Param("column")
	if !s.validColumns[column] {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Invalid column name. Valid columns are: %s", strings.Join(s.columnNames, ", ")),
		})
	}

	// Parse pagination parameters
	limit := 10 // default limit
	if limitStr := c.QueryParam("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	offset := 0 // default offset
	if offsetStr := c.QueryParam("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	value := c.Param("value")
	results, err := s.queryData(column, value, offset, limit)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	if len(results) == 0 {
		if offset == 0 {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "No matching records found"})
		}
		return c.NoContent(http.StatusNoContent) // No more results at this offset
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
		var count int
		if err := rows.Scan(&value, &count); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		results = append(results, map[string]interface{}{
			"value": value,
			"count": count,
		})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"column": column,
		"values": results,
	})
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
	var (
		source = flag.String("s", "", "Source to load (file path or URL)")
		port   = flag.Int("p", 9090, "Port to run HTTP server on")
	)
	flag.Parse()

	if *source == "" {
		fmt.Println("Error: Source is required (use -s to specify a file path or URL)")
		flag.Usage()
		return
	}

	server, err := NewQuackServer(*source)
	if err != nil {
		fmt.Printf("Error initializing server: %v\n", err)
		return
	}
	defer server.Close()

	e := echo.New()

	// API v1 group
	v1 := e.Group("/api/v1")
	v1.GET("/columns", server.handleColumns)
	v1.GET("/query/:column", server.handleColumnValues) // Get unique values and counts
	v1.GET("/query/:column/:value", server.handleQuery) // Get specific value matches

	e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", *port)))
}
