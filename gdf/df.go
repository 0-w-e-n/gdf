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

func NewDataFrame(rows []Row, columns []string, types []string) *DataFrame {
    df := &DataFrame{
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

func (df *DataFrame) Show() {
    log.Println("Columns:", df.Columns)
    log.Println("Types:", df.Types)
    log.Println("Rows:")
    for _, r := range df.Rows {
        r.Show()
    }
}

func (df *DataFrame) Head(size ...int) *DataFrame {
    headSize := 10
    if len(size) > 0 {
        headSize = size[0]
    }

    if len(df.Rows) < headSize {
        return df
    }

    rows := df.Rows
    rows = rows[:headSize]
    newDf := &DataFrame{
        rows,
        df.Columns,
        df.Types,
    }
    return newDf
}

func (df *DataFrame) ContainsColumn(column string) bool {
    foundInDf := false
    for _, col := range df.Columns {
        if col == column {
            foundInDf = true
        }
    }
    return foundInDf
}

func (df *DataFrame) Add(otherDf *DataFrame, column string) *DataFrame {
    err := checkDFCols(df, otherDf, column)
    if err != nil {
        panic(err)
    }

    newDf := &DataFrame{
        df.Rows,
        df.Columns,
        df.Types,
    }
    for i, row := range newDf.Rows {
        otherDfRow := otherDf.Rows[i]
        otherDfVal := otherDfRow.Values[column]
        dfVal := row.Values[column]
        switch dfVal.(type) {
        case int:
            newVal := dfVal.(int) + otherDfVal.(int)
            newDf.Rows[i].Values[column] = newVal
        case float64:
            newVal := dfVal.(float64) + otherDfVal.(float64)
            newDf.Rows[i].Values[column] = newVal
        }
    }
    return newDf
}

func (df *DataFrame) Mul(otherDf *DataFrame, column string) *DataFrame {
    err := checkDFCols(df, otherDf, column)
    if err != nil {
        panic(err)
    }

    newDf := &DataFrame{
        df.Rows,
        df.Columns,
        df.Types,
    }
    for i, row := range newDf.Rows {
        otherDfRow := otherDf.Rows[i]
        otherDfVal := otherDfRow.Values[column]
        dfVal := row.Values[column]
        switch dfVal.(type) {
        case int:
            newVal := dfVal.(int) * otherDfVal.(int)
            newDf.Rows[i].Values[column] = newVal
        case float64:
            newVal := dfVal.(float64) * otherDfVal.(float64)
            newDf.Rows[i].Values[column] = newVal
        }
    }
    return newDf
}

type applyFunc func(*DataFrame) *DataFrame

type GroupedDataFrames struct {
    DataFrames []*DataFrame
}

func (dfs GroupedDataFrames) Apply(fn applyFunc) *DataFrame {
    newDfs := make([]*DataFrame, len(dfs.DataFrames))
    for i, df := range dfs.DataFrames {
        newDfs[i] = fn(df)
    }
    newDf := Concat(newDfs)
    return newDf
}

func (df *DataFrame) GroupBy(columns ...string) GroupedDataFrames {
    if len(columns) == 0 {
        o := make([]*DataFrame, 1)
        o[0] = df
        return GroupedDataFrames{DataFrames: o}
    }
    // TODO: add real implementation
    o := make([]*DataFrame, 1)
    o[0] = df
    return GroupedDataFrames{DataFrames: o}
}

func Concat(dfs []*DataFrame) *DataFrame {
    if len(dfs) == 0 {
        return &DataFrame{}
    }

    firstDf := dfs[0]

    rows := firstDf.Rows

    for i, df := range dfs {
        if i == 0 {
            continue
        }

        for _, column := range df.Columns {
            err := checkDFCols(df, firstDf, column)
            if err != nil {
                panic(err)
            }
            rows = append(rows, df.Rows...)
        }
    }

    newDf := &DataFrame{
        Columns: firstDf.Columns,
        Types: firstDf.Types,
        Rows: rows,
    }

    return newDf
}

func checkDFCols(df1 *DataFrame, df2 *DataFrame, column string) (err error) {
    if !df1.ContainsColumn(column) {
        err = errors.New(column + " not found in source DataFrame 1")
    }

    if !df2.ContainsColumn(column) {
        err = errors.New(column + " not found in DataFrame 2")
    }

    if len(df1.Rows) != len(df2.Rows) {
        err = errors.New("Provided DataFrames are not the same length, cannot perform addition")
    }
    return err
}
