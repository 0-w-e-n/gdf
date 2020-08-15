package main

import (
    "fmt"
    "gopher_df/gdf"
)

func main() {
    vals := make(map[string]interface{})
    vals["test1"] = 1
    vals["test2"] = 10.1
    vals["test"] = "oh boy"
    cols := []string{"test1", "test2", "test"}
    types := []string{"int", "float64", "string"}
    r := gdf.Row(
        cols,
        vals,
        types,
    )
    for i, col := range r.Columns {
        t := r.Types[i]
        val := r.Values[col]
        fmt.Println(col, t, val)
    }

    //rows := []gdf.Row{r}

    //df := gdf.DataFrame
}
