package config

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
)

// LoadEnvFiles reads the .env files of the current environment into the process environment.
//
// LIKHO_ENV (development, staging or production; default development) picks the files. They are
// read in this order, each one overriding the one before, and a variable that is already set in
// the real environment wins over all of them:
//
//	.env  .env.local  .env.<LIKHO_ENV>  .env.<LIKHO_ENV>.local
//
// The .env.<LIKHO_ENV> files are committed and hold no secrets; the .local files are ignored by
// git and hold the secrets of that environment on this machine. A file that does not exist is
// skipped. It returns the names of the files that were read.
func LoadEnvFiles() ([]string, error) {
	env := os.Getenv("LIKHO_ENV")
	if env == "" {
		env = "development"
	}
	already := map[string]bool{}
	for _, pair := range os.Environ() {
		name, _, _ := strings.Cut(pair, "=")
		already[name] = true
	}
	var read []string
	for _, name := range []string{".env", ".env.local", ".env." + env, ".env." + env + ".local"} {
		values, err := parseEnvFile(name)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return read, err
		}
		for key, value := range values {
			if !already[key] {
				_ = os.Setenv(key, value)
			}
		}
		read = append(read, name)
	}
	return read, nil
}

// parseEnvFile reads KEY=value lines. Blank lines and # comments are ignored; a value may be
// quoted with " or ' and an unquoted value may be followed by a # comment.
func parseEnvFile(path string) (map[string]string, error) {
	file, err := os.Open(path) //nolint:gosec // a fixed set of names in the working directory
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		text = strings.TrimPrefix(text, "export ")
		key, value, found := strings.Cut(text, "=")
		key = strings.TrimSpace(key)
		if !found || key == "" || strings.ContainsAny(key, " \t") {
			return nil, fmt.Errorf("%s line %d: expected KEY=value", path, line)
		}
		value = strings.TrimSpace(value)
		switch {
		case len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0]:
			value = value[1 : len(value)-1]
		default:
			if i := strings.Index(value, " #"); i >= 0 {
				value = strings.TrimSpace(value[:i])
			}
		}
		values[key] = value
	}
	return values, scanner.Err()
}
