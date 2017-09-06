package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"

	alog "github.com/apex/log"
	"github.com/avct/apexorc"
)

const (
	words = "/usr/share/dict/words"
	out   = "out.orc"
)

func rot13(r rune) rune {
	if r >= 'a' && r <= 'z' {
		// Rotate lowercase letters 13 places.
		if r >= 'm' {
			return r - 13
		} else {
			return r + 13
		}
	} else if r >= 'A' && r <= 'Z' {
		// Rotate uppercase letters 13 places.
		if r >= 'M' {
			return r - 13
		} else {
			return r + 13
		}
	}
	// Do nothing.
	return r
}

func main() {
	f, err := os.Open(words)
	if err != nil {
		panic(err.Error())
	}
	defer f.Close()
	rand.Seed(42)
	scanner := bufio.NewScanner(f)
	handler := apexorc.NewHandler("out.orc")
	alog.SetHandler(handler)
	levelCount := 0
	var wordy string
	var randy string
	var mapped string
	var ctx *alog.Entry

	for scanner.Scan() {
		wordy = scanner.Text()
		randy = strconv.Itoa(rand.Int())
		levelCount++
		if levelCount > 3 {
			levelCount = 0
		}
		ctx = alog.WithFields(
			alog.Fields{
				"word": wordy,
				"rand": randy,
			})
		switch levelCount {
		case 0:
			ctx.Debug(fmt.Sprintf("Can you say %q, %s times on one breath?", wordy, randy))
		case 1:
			ctx.Info(fmt.Sprintf("Dang, I can put %s times as much on my %s as yesterday!", randy, wordy))
		case 2:
			ctx.Warn(fmt.Sprintf("Didn't your mother tell you? Rule %s is never eat %s!", randy, wordy))
		case 3:
			mapped = strings.Map(rot13, wordy)
			err = fmt.Errorf("Error: %s, %s failure", randy, wordy)
			ctx.WithError(err).Error(fmt.Sprintf("It's all gone to %s", mapped))
		}
	}
	handler.Close()
}
