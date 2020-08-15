package main

import (
    "gopher_df/gdf"
)

func main() {
    cols := []string{"A", "B", "C"}
    types := []string{"int", "float64", "string"}
    rows := []gdf.Row{}

    for i := 1;  i < 1000000; i++ {
        vals := make(map[string]interface{})
        vals["A"] = i
        vals["B"] = float64(i) * 1.2
        vals["C"] = ":)"
        r := gdf.NewRow(cols, vals, types)
        rows = append(rows, r)
    }
    df := gdf.NewDataFrame(rows, cols, types)
    df.Show()
}
