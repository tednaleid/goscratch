package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	_ "github.com/marcboeker/go-duckdb"
)

type QuackServer struct {
	db           *sql.DB
	validColumns map[string]bool
	columnNames  []string
}

func NewQuackServer(csvFile string) (*QuackServer, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("opening DuckDB: %w", err)
	}

	if err := createTable(db, csvFile); err != nil {
		db.Close()
		return nil, err
	}

	columns, err := getValidColumns(db)
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
	}, nil
}

func (s *QuackServer) Close() error {
	return s.db.Close()
}

func createTable(db *sql.DB, csvFile string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE TABLE data AS SELECT * FROM read_csv_auto('%s')", csvFile))
	if err != nil {
		return fmt.Errorf("creating table from CSV: %w", err)
	}
	return nil
}

func getValidColumns(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT * FROM data LIMIT 0")
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

func (s *QuackServer) handleQuery(c echo.Context) error {
	column := camelToSnake(c.Param("column"))
	value := c.Param("value")

	if !s.validColumns[column] {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Invalid column name. Valid columns are: %s", strings.Join(s.columnNames, ", ")),
		})
	}

	results, err := s.queryData(column, value)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	if len(results) == 0 {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "No matching records found"})
	}

	return c.JSON(http.StatusOK, results)
}

func (s *QuackServer) queryData(column, value string) ([]map[string]interface{}, error) {
	rows, err := s.db.Query(fmt.Sprintf("SELECT * FROM data WHERE %s = ?", column), value)
	if err != nil {
		return nil, fmt.Errorf("querying data: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting result columns: %w", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		result, err := scanRow(rows, cols)
		if err != nil {
			return nil, err
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

// camelToSnake converts a camelCase string to snake_case
func camelToSnake(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && 'A' <= r && r <= 'Z' {
			result.WriteRune('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

func main() {
	var (
		csvFile = flag.String("f", "", "CSV file to load")
		port    = flag.Int("p", 9090, "Port to run HTTP server on")
	)
	flag.Parse()

	if *csvFile == "" {
		fmt.Println("Error: CSV file is required")
		flag.Usage()
		return
	}

	server, err := NewQuackServer(*csvFile)
	if err != nil {
		fmt.Printf("Error initializing server: %v\n", err)
		return
	}
	defer server.Close()

	e := echo.New()
	e.GET("/:column/:value", server.handleQuery)
	e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", *port)))
}
