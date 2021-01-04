# gdf (Gopher DataFrames)

I thought it'd be interesting to try and create a DataFrame in Golang so I started working on gdf, hoping to one day have enough functionallity to do some basic data wrangling with a nice pandas-like-API (drawing some inspiration from Spark's API as well but there's no plans to make this a distributed system). Particularly interested in seeing how fast go would be for basic tasks one would do in pandas (filtering, joining, etc). Would ideally like to use go routines to create a faster split-apply-combine than in pandas particularly for larger DataFrames.

Run tests: `$ go test ./...`
