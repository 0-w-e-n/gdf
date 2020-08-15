package gdf

import (
    "errors"
)

type dataFrame struct {
    Rows []row
    Columns []string
    Types []string
}

func DataFrame(rows []row, columns []string, types []string) dataFrame {
    df := dataFrame{
       rows,
       columns,
       types,
    }
    for rowNum, r := range df.Rows {
        for i, col := range df.Columns {
            rowCol := r.Columns[i]
            if rowCol != col {
                errMsg := "Row " + string(rowNum) + " has mismatched column, expected: " + col
                errMsg = errMsg + " but got: " + rowCol
                err := errors.New(errMsg)
                panic(err)
            }

            rowType := r.Types[i]
            dfType := df.Types[i]
            if rowType != dfType {
                errMsg := "Row " + string(rowNum) + " has mismatched type, expected: " + dfType
                errMsg = errMsg + " but got: " + rowType
                err := errors.New(errMsg)
                panic(err)
            }
        }
    }
    return df
}
