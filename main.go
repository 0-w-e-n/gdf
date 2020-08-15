package main

import (
    "gopher_df/gdf"
)

func main() {
    vals := make(map[string]interface{})
    vals["test1"] = 1
    vals["test2"] = 10.1
    vals["test"] = "oh boy"

    vals2 := make(map[string]interface{})
    vals2["test1"] = 12
    vals2["test2"] = 19.19
    vals2["test"] = ":)"

    cols := []string{"test1", "test2", "test"}
    types := []string{"int", "float64", "string"}
    r := gdf.NewRow(cols, vals, types)
    r.Show()
    r2 := gdf.NewRow(cols, vals2, types)
    r2.Show()
    rows := []gdf.Row{r, r2}

    df := gdf.NewDataFrame(rows, cols, types)
    df.Show()
}
