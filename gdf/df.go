package gdf

import (
    "errors"
    "strconv"
    "log"
    "fmt"
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
                errMsg := "Row " + fmt.Sprint(rowNum) + " has mismatched column, expected: " + col
                errMsg = errMsg + " but got: " + rowCol
                err := errors.New(errMsg)
                panic(err)
            }

            rowType := r.Types[i]
            dfType := df.Types[i]
            if rowType != dfType {
                errMsg := "Row " + fmt.Sprint(rowNum) + " has mismatched type, expected: " + dfType
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
    if len(df.Rows) != len(otherDf.Rows) {
	err = errors.New("Provided DataFrames are not the same length, cannot perform addition")
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
    if len(df.Rows) != len(otherDf.Rows) {
	err = errors.New("Provided DataFrames are not the same length, cannot perform addition")
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

type ApplyFunc func(*DataFrame) *DataFrame

type GroupedDataFrames struct {
    DataFrames []*DataFrame
}

func (dfs GroupedDataFrames) Apply(fn ApplyFunc) *DataFrame {
    lenDfs := len(dfs.DataFrames)
    newDfs := make([]*DataFrame, lenDfs)
    results := make(chan *DataFrame)
    for _, df := range dfs.DataFrames {
        _df := &DataFrame{
            Rows: df.Rows,
            Types: df.Types,
            Columns: df.Columns,
        }
        go func() {
            res := fn(_df)
            results <- res
        }()
    }

    index := 0
    for res := range results {
        newDfs[index] = res
        index = index + 1
        if index == lenDfs {
            close(results)
        }
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
    groups := make(map[string]*DataFrame)
    for _, row := range df.Rows {
        uid := ""
        for i, col := range columns {
            val := row.Values[col]
            switch val.(type) {
            case int:
                tVal := val.(int)
                uid = uid + fmt.Sprint(tVal)
            case float64:
                tVal := val.(float64)
                sVal := strconv.FormatFloat(tVal, 'f', 6, 64)
                uid = uid + fmt.Sprint(sVal)
            case string:
                uid = uid + val.(string)
            }
            if i != len(columns) - 1 {
                uid = uid + "_"
            }
        }
        cDf, found := groups[uid]
        if !found {
            nRows := []Row{row}
            nDf := &DataFrame{
                Rows: nRows,
                Types: df.Types,
                Columns: df.Columns,
            }
            groups[uid] = nDf
        } else {
            nRows := cDf.Rows
            nRows = append(nRows, row)
            nDf := &DataFrame{
                Rows: nRows,
                Types: cDf.Types,
                Columns: cDf.Columns,
            }
            groups[uid] = nDf
        }
    }
    dfs := make([]*DataFrame, len(groups))
    index := 0
    for _, df_ := range groups {
        dfs[index] = df_
        index = index + 1
    }
    return GroupedDataFrames{DataFrames: dfs}
}

func Concat(dfs []*DataFrame) *DataFrame {
    if len(dfs) == 0 {
        err := errors.New("Empty slice of DataFrames passed, need at least one to concat")
        panic(err)
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

    return err
}
