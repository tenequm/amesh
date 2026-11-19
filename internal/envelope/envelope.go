package envelope

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Message struct {
	From, FromSession, FromName, FromMode string
	HopChain                              []string
	Body                                  string
}

const (
	tag     = "cross-session-message"
	session = "[A-Za-z0-9_-]{1,80}"
	hops    = "[0-9a-f]{24}(?:,[0-9a-f]{24}){0,31}"
	address = "uds:[A-Za-z0-9%:_/.-]+"
)

var (
	sessionRE = regexp.MustCompile("^" + session + "$")
	hopsRE    = regexp.MustCompile("^" + hops + "$")
	headRE    = regexp.MustCompile("^<" + tag + `(?: from="(` + address + `)")?(?: from-session="(` + session + `)")?(?: hop-chain="(` + hops + `)")?(?: from-name="([^"<>\r\n]+)")?(?: from-mode="(bypass|prompting)")?>$`)
	closeRE   = regexp.MustCompile(`(?i)<[\s\p{Z}\x0b\x{85}]*/[\s\p{Z}\x0b\x{85}]*cross`)
)

func cleanName(s string) string {
	s = strings.TrimSpace(strings.Map(func(r rune) rune {
		if r == '"' || r == '<' || r == '>' || unicode.In(r, unicode.Cc, unicode.Cf, unicode.Cs, unicode.Zl, unicode.Zp) {
			return -1
		}
		return r
	}, s))
	if chars := []rune(s); len(chars) > 64 {
		return string(chars[:64]) + "\u2026"
	}
	return s
}

// Address takes a raw socket path. Message.From carries the resulting wire address.
func Address(path string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.WriteString("uds:")
	for i := range len(path) {
		c := path[i]
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || strings.IndexByte(":_/.-", c) >= 0 {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&15])
		}
	}
	return b.String()
}

var addressRE = regexp.MustCompile("^" + address + "$")

func Render(message Message) (string, error) {
	if !utf8.ValidString(message.Body) || !utf8.ValidString(message.FromName) {
		return "", errors.New("invalid UTF-8 in envelope")
	}
	var b strings.Builder
	b.WriteString("<" + tag)
	attr := func(key, value string) {
		if value != "" {
			b.WriteString(" " + key + "=\"" + value + "\"")
		}
	}
	if from := message.From; from != "" && (!addressRE.MatchString(from) || len(from) > 300) {
		return "", errors.New("invalid sender address")
	}
	attr("from", message.From)
	if sessionRE.MatchString(message.FromSession) {
		attr("from-session", message.FromSession)
	}
	if chain := strings.Join(message.HopChain, ","); hopsRE.MatchString(chain) {
		attr("hop-chain", chain)
	} else if len(message.HopChain) != 0 {
		return "", errors.New("invalid hop chain")
	}
	attr("from-name", cleanName(message.FromName))
	if message.FromMode != "" && message.FromMode != "bypass" && message.FromMode != "prompting" {
		return "", errors.New("invalid sender permission class")
	}
	attr("from-mode", message.FromMode)
	b.WriteString(">\n")
	b.WriteString(closeRE.ReplaceAllStringFunc(message.Body, func(match string) string { return "&lt;" + match[1:] }))
	b.WriteString("\n</" + tag + ">")
	return b.String(), nil
}

func Parse(content string) (Message, error) {
	head, rest, ok := strings.Cut(content, "\n")
	body, end := strings.CutSuffix(rest, "\n</"+tag+">")
	fields := headRE.FindStringSubmatch(head)
	if !ok || !end || fields == nil {
		return Message{}, errors.New("malformed native envelope")
	}
	m := Message{From: fields[1], FromSession: fields[2], FromName: fields[4], FromMode: fields[5], Body: body}
	if fields[3] != "" {
		m.HopChain = strings.Split(fields[3], ",")
	}
	rendered, err := Render(m)
	if err != nil || rendered != content {
		return Message{}, errors.New("noncanonical native envelope")
	}
	return m, nil
}

func SelfCheck() error {
	for _, v := range vectors {
		content, err := Render(v.Input)
		if err != nil || content != v.Content {
			return fmt.Errorf("native envelope %s failed to render", v.Name)
		}
		got, err := Parse(v.Content)
		if err != nil || got.Body != v.ParsedBody || got.FromName != v.ParsedName || got.From != v.Input.From || got.FromSession != v.Input.FromSession || got.FromMode != v.Input.FromMode || !slices.Equal(got.HopChain, v.Input.HopChain) {
			return fmt.Errorf("native envelope %s failed to parse", v.Name)
		}
	}
	return nil
}
