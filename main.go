package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type Input struct {
	Params []struct {
		InputName string `json:"inputname"`
		CompValue string `json:"compvalue"`
	} `json:"params"`
}

type Output struct {
	Result interface{} `json:"result"`
	Error  string      `json:"error"`
}

func main() {
	var input Input
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("failed to decode input: %v", err)})
		return
	}

	var (
		host       string
		port       int
		username   string
		password   string
		dbname     string
		dataType   = "query" // query, table, stored_procedure, stored_function
		objectName string
		query      string
		parameters string // JSON array of arguments
	)

	// Extract parameters
	for _, p := range input.Params {
		val := strings.TrimSpace(p.CompValue)
		switch strings.ToLower(p.InputName) {
		case "host":
			host = val
		case "port":
			fmt.Sscanf(val, "%d", &port)
		case "username":
			username = val
		case "password":
			password = val
		case "dbname":
			dbname = val
		case "data_type":
			if val != "" {
				dataType = strings.ToLower(val)
			}
		case "object_name":
			objectName = val
		case "query":
			query = val
		case "parameters":
			parameters = val
		}
	}

	// Validate connection params
	if host == "" || username == "" || dbname == "" {
		json.NewEncoder(os.Stdout).Encode(Output{Error: "host, username, and dbname are required"})
		return
	}
	if port == 0 {
		port = 3306
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", username, password, host, port, dbname)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("failed to connect: %v", err)})
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("failed to ping db: %v", err)})
		return
	}

	var rows *sql.Rows
	var execResult sql.Result
	isSelect := false

	switch dataType {
	case "table":
		if objectName == "" {
			json.NewEncoder(os.Stdout).Encode(Output{Error: "object_name is required for table"})
			return
		}
		// Basic SELECT * FROM table limiting mostly for safety? No, let's dump all.
		rows, err = db.Query(fmt.Sprintf("SELECT * FROM %s", objectName))
		isSelect = true

	case "stored_procedure":
		if objectName == "" {
			json.NewEncoder(os.Stdout).Encode(Output{Error: "object_name is required for stored_procedure"})
			return
		}
		args, err := parseArgs(parameters)
		if err != nil {
			json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("invalid parameters: %v", err)})
			return
		}

		placeholders := make([]string, len(args))
		for i := range args {
			placeholders[i] = "?"
		}

		q := fmt.Sprintf("CALL %s(%s)", objectName, strings.Join(placeholders, ","))
		rows, err = db.Query(q, args...)
		// Stored procedures might return rows or might just execute.
		// If it has a result set, current driver should handle it via Query.
		// If no result set, it might error "no rows in result set" or return empty.
		isSelect = true

	case "stored_function":
		if objectName == "" {
			json.NewEncoder(os.Stdout).Encode(Output{Error: "object_name is required for stored_function"})
			return
		}
		args, err := parseArgs(parameters)
		if err != nil {
			json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("invalid parameters: %v", err)})
			return
		}

		placeholders := make([]string, len(args))
		for i := range args {
			placeholders[i] = "?"
		}

		// SELECT func(args)
		q := fmt.Sprintf("SELECT %s(%s)", objectName, strings.Join(placeholders, ","))
		rows, err = db.Query(q, args...)
		isSelect = true

	case "query":
		fallthrough
	default:
		if query == "" {
			json.NewEncoder(os.Stdout).Encode(Output{Error: "query is required"})
			return
		}
		isSelect = strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT")
		// Also SHOW, DESCRIBE, EXPLAIN are queries
		if !isSelect {
			cmd := strings.ToUpper(strings.TrimSpace(query))
			if strings.HasPrefix(cmd, "SHOW") || strings.HasPrefix(cmd, "DESCRIBE") || strings.HasPrefix(cmd, "EXPLAIN") || strings.HasPrefix(cmd, "CALL") {
				isSelect = true
			}
		}

		if isSelect {
			rows, err = db.Query(query)
		} else {
			execResult, err = db.Exec(query)
		}
	}

	if err != nil {
		json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("execution error: %v", err)})
		return
	}

	if isSelect && rows != nil {
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("columns error: %v", err)})
			return
		}

		results := make([]map[string]interface{}, 0)
		for rows.Next() {
			columnPointers := make([]interface{}, len(columns))
			for i := range columns {
				columnPointers[i] = new(interface{})
			}

			if err := rows.Scan(columnPointers...); err != nil {
				json.NewEncoder(os.Stdout).Encode(Output{Error: fmt.Sprintf("scan error: %v", err)})
				return
			}

			m := make(map[string]interface{})
			for i, colName := range columns {
				val := *(columnPointers[i].(*interface{}))

				// Handle []byte for strings
				if b, ok := val.([]byte); ok {
					m[colName] = string(b)
				} else {
					m[colName] = val
				}
			}
			results = append(results, m)
		}
		json.NewEncoder(os.Stdout).Encode(Output{Result: results})

	} else if execResult != nil {
		id, _ := execResult.LastInsertId()
		affected, _ := execResult.RowsAffected()
		json.NewEncoder(os.Stdout).Encode(Output{Result: map[string]int64{
			"last_insert_id": id,
			"rows_affected":  affected,
		}})
	} else {
		json.NewEncoder(os.Stdout).Encode(Output{Result: "OK"})
	}
}

func parseArgs(paramStr string) ([]interface{}, error) {
	if paramStr == "" {
		return []interface{}{}, nil
	}
	var args []interface{}
	// Try parsing as JSON array
	if err := json.Unmarshal([]byte(paramStr), &args); err != nil {
		// If not JSON, maybe treat as single string? Or CSV?
		// For now strictly JSON array for complex types support
		return nil, err
	}
	return args, nil
}
