##### SAMPLE RUN COMMAND

`python phoenix/prototype/geo_country_stream.py --pub "testOut" --sub "testIn"`

#### Sample run alongwith city level geocoder
`python phoenix/prototype/geo_country_stream.py --pub "--" --sub "testIn" | python phoenix/prototype/geo_code_stream.py --pub "--" `

#### Arguments
`--cat read from stdin and write to stdout`
`--region name of region to filter by (LA or MENA ). MENA is not yet implemented `
