package bbq

import "cloud.google.com/go/bigquery"

type CustomSaver struct {
	values map[string]float64
}

func (s *CustomSaver) Save() (map[string]bigquery.Value, string, error) {
	var row = make(map[string]bigquery.Value, len(s.values))
	for k, v := range s.values {
		row[k] = v
	}
	return row, "", nil
}
