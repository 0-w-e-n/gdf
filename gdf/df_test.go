package gdf

import (
    "testing"
)

func TestDataFrame(t *testing.T) {
    cols := []string{"A", "B", "C"}
    types := []string{"int", "float64", "string"}
    rows := []Row{}
    rows2 := []Row{}

    smilePad := ")"
    // make a ton of fake data
    for i := 1;  i < 50000; i++ {
        if i % 10000 == 0 {
            // make a bunch of groups
            smilePad = smilePad + ")"
        }

        vals := make(map[string]interface{})
        vals["A"] = i * 7
        vals["B"] = float64(i) * 1.2
        vals["C"] = ":" + smilePad
        r := NewRow(cols, vals, types)
        rows = append(rows, r)

        vals2 := make(map[string]interface{})
        vals2["A"] = i * 13
        vals2["B"] = float64(i) * 12.33
        vals2["C"] = ":)" + smilePad
        r2 := NewRow(cols, vals2, types)
        rows2 = append(rows2, r2)
    }
    df := NewDataFrame(rows, cols, types).
    Mul(NewDataFrame(rows2, cols, types), "B").
    Add(NewDataFrame(rows2, cols, types), "A")

    dfs := make([]*DataFrame, 2)
    dfs[1] = df
    dfs[0] = NewDataFrame(rows2, cols, types)

    ndf := Concat(dfs)

    fn := func(df *DataFrame) *DataFrame {
        df = df.Add(df, "A")
        return df
    }

    ndf.
    GroupBy("C").
    Apply(fn).
    Head(1000).
    Show()
}
