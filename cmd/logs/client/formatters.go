package client

import (
	"encoding/json"
	"text/template"
)

var formattingFuncMap = template.FuncMap{
	"json": formatJSON,
}

func formatJSON(i interface{}) string {
	bs, _ := json.Marshal(i)
	return string(bs)
}
