// Package relabel implements Prometheus-compatible relabeling for metric labels
// and Loki stream labels, driven by config.RelabelConfig rules.
package relabel

import (
	"regexp"
	"strings"

	cfg "github.com/mikeosude/ome-kafka-bridge/internal/config"
)

// compiledRule is a RelabelConfig with its regex pre-compiled.
type compiledRule struct {
	cfg.RelabelConfig
	re *regexp.Regexp
}

// Engine applies a list of relabeling rules.
type Engine struct {
	rules []compiledRule
}

// New compiles all relabeling rules. Returns an error if any regex is invalid.
func New(rules []cfg.RelabelConfig) (*Engine, error) {
	compiled := make([]compiledRule, 0, len(rules))
	for _, r := range rules {
		regex := r.Regex
		if regex == "" {
			regex = "(.*)"
		}
		if r.Replacement == "" {
			r.Replacement = "$1"
		}
		if r.Separator == "" {
			r.Separator = ";"
		}
		re, err := regexp.Compile("^(?:" + regex + ")$")
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, compiledRule{r, re})
	}
	return &Engine{rules: compiled}, nil
}

// Apply runs the engine's rules against a label set, modifying it in-place.
// Returns false if a "drop" rule matched (caller should discard the sample).
func (e *Engine) Apply(labels map[string]string) bool {
	for _, rule := range e.rules {
		switch strings.ToLower(rule.Action) {
		case "drop":
			val := joinValues(labels, rule.SourceLabels, rule.Separator)
			if rule.re.MatchString(val) {
				return false
			}
		case "keep":
			val := joinValues(labels, rule.SourceLabels, rule.Separator)
			if !rule.re.MatchString(val) {
				return false
			}
		case "replace", "":
			val := joinValues(labels, rule.SourceLabels, rule.Separator)
			if !rule.re.MatchString(val) {
				continue
			}
			replacement := rule.re.ReplaceAllString(val, rule.Replacement)
			if rule.TargetLabel != "" {
				if replacement == "" {
					delete(labels, rule.TargetLabel)
				} else {
					labels[rule.TargetLabel] = replacement
				}
			}
		case "labelmap":
			for k, v := range labels {
				if rule.re.MatchString(k) {
					newKey := rule.re.ReplaceAllString(k, rule.Replacement)
					labels[newKey] = v
				}
			}
		case "labeldrop":
			for k := range labels {
				if rule.re.MatchString(k) {
					delete(labels, k)
				}
			}
		case "labelkeep":
			for k := range labels {
				if !rule.re.MatchString(k) {
					delete(labels, k)
				}
			}
		}
	}
	return true
}

func joinValues(labels map[string]string, keys []string, sep string) string {
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, labels[k])
	}
	return strings.Join(parts, sep)
}
