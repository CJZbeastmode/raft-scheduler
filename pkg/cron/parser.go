package cron

import (
    "fmt"
    "math/bits"
    "strconv"
    "strings"
    "time"
)


// Expression holds the parsed cron fields as bitmasks.
// Each bit position represents a valid value for that field.
type Expression struct {
	second		uint64 // 0–59
	minute		uint64 // 0–59
	hour		uint64 // 0–23
	dayOfMonth	uint64 // 1-31
	month		uint64 // 1-12
	dayOfWeek	uint64 // 0-6 (Sun=0)
}


// Parse parses a 5-field cron expression (min hour dom month dow).
// Returns an error if the expression is invalid.
func Parse(expr string) (*Expression, error) {
	// Get field
	fields := strings.Fields(expr)
	
	// Cron expect 5 fields
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields, got %d", len(fields))
	}

	e := &Expression{}
	var err error

	// Parse and return
	if e.minute,     err = parseField(fields[0], 0, 59); err != nil { return nil, fmt.Errorf("minute: %w", err) }
    if e.hour,       err = parseField(fields[1], 0, 23); err != nil { return nil, fmt.Errorf("hour: %w", err) }
    if e.dayOfMonth, err = parseField(fields[2], 1, 31); err != nil { return nil, fmt.Errorf("dom: %w", err) }
    if e.month,      err = parseField(fields[3], 1, 12); err != nil { return nil, fmt.Errorf("month: %w", err) }
    if e.dayOfWeek,  err = parseField(fields[4], 0, 6);  err != nil { return nil, fmt.Errorf("dow: %w", err) }
    return e, nil
}


// Find the next future moment that matches the cron expression
func (e *Expression) Next(t time.Time) time.Time {
	t = t.Truncate(time.Minute).Add(time.Minute)		// move forward to prevent returning itself
														// Truncate: remove everythnig below second

	// search up to 4 years to avoid infinite loop on impossible expressions
	limit := t.AddDate(4, 0, 0)

	// loop until expiration
	for t.Before(limit) {
		// month
		if e.month & (1<<uint(t.Month())) == 0 {
			t = advanceMonth(t)
			continue
		}

		// day: both dom and dow must allow it (standard vixie-cron semantics: if
        // both are restricted, either match is sufficient; if both are *, any day)
        domMatch := e.dayOfMonth & (1<<uint(t.Day())) != 0
		dowMatch := e.dayOfWeek & (1<<uint(t.Weekday())) != 0
		if !domMatch && !dowMatch {
			t = t.AddDate(0, 0, 1)
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
			continue
		}

		// hour
		if e.hour & (1<<uint(t.Hour())) == 0 {
			t = advanceHour(t)
			continue
		}

		// minute
		if e.minute & (1<<uint(t.Minute())) == 0 {
			t = t.Add(time.Minute).Truncate(time.Minute)
			continue
		}

		// return if match
		return t
	}
	return time.Time{} // no match found
}

// --- field parser ---
func parseField(field string, min, max int) (uint64, error) {
	// parse "all"
	if field == "*" {
		return fillRangeStep(min, max, 1), nil				// fill range but step being 1 as default
	}

	// parse each part
	var mask uint64
	for _, part := range strings.Split(field, ",") {		// split by ","
		m, err := parsePart(part, min, max)
		if err != nil {
			return 0, err
		}
		mask |= m
	}
	return mask, nil
}


func parsePart(part string, min, max int) (uint64, error) {
	// parse step
	// step: */2 or 1-5/2
	// only changes if the input has a "/" in it
	step := 1
	if idx := strings.Index(part, "/"); idx >= 0 {
		s, err := strconv.Atoi(part[idx + 1:])		// slices everything after the "/"
		// fail it the thing after / wasn't a number, or it is 0 or negative
		if err != nil || s <= 0 {
			return 0, fmt.Errorf("invalid step %q", part[idx+1:])
		}
		step = s			// save the step value
		part = part[:idx]	// ignore the part after "/"
	}
	
    // parse lo and hi
    lo, hi := min, max
    if part != "*" {
		// looks for hyphen
		if idx := strings.Index(part, "-"); idx >= 0 {
			// split on the hyphen and parse both sides for lo and hi
			var err error
			if lo, err = strconv.Atoi(part[:idx]); err != nil {
                return 0, fmt.Errorf("invalid range start %q", part[:idx])
            }
            if hi, err = strconv.Atoi(part[idx+1:]); err != nil {
                return 0, fmt.Errorf("invalid range end %q", part[idx+1:])
            }
		} else {
			// parse the value
            v, err := strconv.Atoi(part)
            if err != nil {
                return 0, fmt.Errorf("invalid value %q", part)
            }
            lo, hi = v, v
        }
    }
	// invalid lo and hi
    if lo < min || hi > max || lo > hi {
        return 0, fmt.Errorf("value %d-%d out of range [%d,%d]", lo, hi, min, max)
    }
	// fill range using lo and hi and step
    return fillRangeStep(lo, hi, step), nil
}


func fillRangeStep(lo, hi, step int) uint64 {
	// loops from lo to hi, jumps by step
	// e.g.: fillRangeStep(0, 59, 15)  →  sets bits 0, 15, 30, 45
	var mask uint64
	for i := lo; i <= hi; i += step {
		mask |= 1 << uint(i)
	}
	return mask
}


// --- time helpers ---

func advanceMonth(t time.Time) time.Time {
    return time.Date(t.Year(), t.Month() + 1, 1, 0, 0, 0, 0, t.Location())
}

func advanceHour(t time.Time) time.Time {
    next := t.Add(time.Hour).Truncate(time.Hour)
    return time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), 0, 0, 0, t.Location())
}

// nextBit returns the position of the lowest set bit >= pos in mask, or -1.
func nextBit(mask uint64, pos int) int {
    shifted := mask >> uint(pos)
    if shifted == 0 {
        return -1
    }
    return pos + bits.TrailingZeros64(shifted)
}

