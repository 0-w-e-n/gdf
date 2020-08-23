package main

import (
    "gopher_df/gdf"
)

func main() {
    cols := []string{"A", "B", "C"}
    types := []string{"int", "float64", "string"}
    rows := []gdf.Row{}
    rows2 := []gdf.Row{}

    smilePad := ")"

    for i := 1;  i < 5000000; i++ {
        if i % 10000 == 0 {
            // make a bunch of groups
            smilePad = smilePad + ")"
        }

        vals := make(map[string]interface{})
        vals["A"] = i * 7
        vals["B"] = float64(i) * 1.2
        vals["C"] = ":" + smilePad
        r := gdf.NewRow(cols, vals, types)
        rows = append(rows, r)

        vals2 := make(map[string]interface{})
        vals2["A"] = i * 13
        vals2["B"] = float64(i) * 12.33
        vals2["C"] = ":)" + smilePad
        r2 := gdf.NewRow(cols, vals2, types)
        rows2 = append(rows2, r2)
    }
    df := gdf.NewDataFrame(rows, cols, types).
    Mul(gdf.NewDataFrame(rows2, cols, types), "B").
    Add(gdf.NewDataFrame(rows2, cols, types), "A")

    dfs := make([]*gdf.DataFrame, 2)
    dfs[1] = df
    dfs[0] = gdf.NewDataFrame(rows2, cols, types)

    ndf := gdf.Concat(dfs)

    fn := func(df *gdf.DataFrame) *gdf.DataFrame {
        df = df.Add(df, "A")
        return df
    }

    ndf.
    GroupBy("C").
    Apply(fn).
    Head(1000).
    Show()
}
