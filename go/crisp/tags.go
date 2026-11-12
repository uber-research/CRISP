package crisp

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ParseTagFilters parses the --tags value: a list of
// {name, value, search_depth} tag filters, with the checks of
// process_trace.py's YAMLAction (exactly these three keys; name and value
// are strings, search_depth an integer).
//
// The Python CLI reads the list as YAML. This port reads JSON, which is also
// valid YAML, so the same value works for both:
//
//	--tags '[{"name": "env", "value": "prod", "search_depth": 1}]'
func ParseTagFilters(s string) ([]TagFilter, error) {
	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	dec.UseNumber()
	var items []map[string]any
	if err := dec.Decode(&items); err != nil {
		return nil, fmt.Errorf("--tags: want a JSON list of {name, value, search_depth}: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("--tags: want a single JSON list of {name, value, search_depth}")
	}
	filters := make([]TagFilter, 0, len(items))
	for _, item := range items {
		name, nameOK := item["name"].(string)
		value, valueOK := item["value"].(string)
		number, _ := item["search_depth"].(json.Number)
		depth, err := number.Int64()
		if len(item) != 3 || !nameOK || !valueOK || err != nil {
			return nil, fmt.Errorf("--tags: want {name: string, value: string, search_depth: integer}, got %v", item)
		}
		filters = append(filters, TagFilter{Name: name, Value: value, SearchDepth: int(depth)})
	}
	return filters, nil
}
