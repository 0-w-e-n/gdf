package gdf

import (
    "errors"
    "log"
    "gopher_df/core"
)

type Row struct {
    Columns []string
    Values map[string]interface{}
    Types []string
}

func NewRow(columns []string, values map[string]interface{}, types []string) Row {
    r := Row{
        Columns: columns,
        Types: types,
        Values: values,
    }
    for i, col := range r.Columns {
        t := r.Types[i]
        val := r.Values[col]
        actualType := core.TypeOf(val)
        isCorrectType := actualType == t
        if !isCorrectType {
            errMsg := "Incorrect type for column " + col + ", expected type is "
            errMsg = errMsg + t + ", but actual type is " + actualType
            err := errors.New(errMsg)
            panic(err)
        }
    }
    return r
}

func (r Row) Show() {
    for i, col := range r.Columns {
        t := r.Types[i]
        val := r.Values[col]
        log.Println("Name:", col, "Type:", t, "Value:", val)
    }
    log.Println()
}
