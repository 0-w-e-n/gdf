package gdf

import (
    "errors"
    "log"
)

type DataFrame struct {
    Rows []Row
    Columns []string
    Types []string
}

func NewDataFrame(rows []Row, columns []string, types []string) DataFrame {
    df := DataFrame{
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

func (df DataFrame) Show() {
    log.Println("Columns:", df.Columns)
    log.Println("Types:", df.Types)
    log.Println("Rows:")
    for _, r := range df.Rows {
        r.Show()
    }
}
