package gdf

import (
    "errors"
    "gopher_df/core"
)

type row struct {
    Columns []string
    Values map[string]interface{}
    Types []string
}

func Row(columns []string, values map[string]interface{}, types []string) row {
    r := row{
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
