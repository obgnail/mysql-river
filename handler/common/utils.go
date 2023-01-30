package common

import "strings"

func List2Map(slice []string) map[string]struct{} {
	res := make(map[string]struct{})
	for _, ele := range slice {
		res[strings.ToLower(ele)] = struct{}{}
	}
	return res
}
