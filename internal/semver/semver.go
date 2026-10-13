// Package semver checks v-prefixed release tag syntax and computes stable
// version successors for the release command. NextStable uses machine int
// arithmetic; it is not an arbitrary-precision version calculator.
package semver

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

var tagPattern = regexp.MustCompile(
	`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)` +
		`(-((0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)` +
		`(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?` +
		`(\+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?$`,
)

var stableTagPattern = regexp.MustCompile(
	`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`,
)

// ValidateTag accepts v-prefixed SemVer syntax, including prerelease/build
// metadata, but does not check component arithmetic range or existing Git tags.
func ValidateTag(tag string) error {
	if !tagPattern.MatchString(tag) {
		return errors.New("tag must be a v-prefixed semantic version")
	}
	return nil
}

// NextStable accepts stable v-prefixed tags and increments patch, minor or major,
// resetting subordinate components. Parsed components use strconv.Atoi with
// ignored range errors and unchecked machine-int increment; use representable
// components with room for the selected increment. No Git state is consulted.
func NextStable(tag, part string) (string, error) {
	matches := stableTagPattern.FindStringSubmatch(tag)
	if matches == nil {
		return "", errors.New("current tag must be a stable semantic version")
	}
	major, _ := strconv.Atoi(matches[1])
	minor, _ := strconv.Atoi(matches[2])
	patch, _ := strconv.Atoi(matches[3])
	switch part {
	case "patch":
		patch++
	case "minor":
		minor++
		patch = 0
	case "major":
		major++
		minor, patch = 0, 0
	default:
		return "", errors.New("release part must be patch, minor, or major")
	}
	return fmt.Sprintf("v%d.%d.%d", major, minor, patch), nil
}
