package cron

import (
    "testing"
    "time"
)

func mustParse(t *testing.T, expr string) *Expression {
    t.Helper()				// marks mustParse as a test helper function
    e, err := Parse(expr)
    if err != nil {
        t.Fatalf("Parse(%q): %v", expr, err)
    }
    return e
}

var base = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

func TestEveryMinute(t *testing.T) {
    e := mustParse(t, "* * * * *")
    actual := e.Next(base)
    expected := base.Add(time.Minute)
    if !actual.Equal(expected) {
        t.Errorf("got %v, want %v", actual, expected)
    }
}

func TestSpecificTime(t *testing.T) {
    e := mustParse(t, "30 9 * * *") // 09:30 every day
    actual := e.Next(base)
    expected := time.Date(2024, 1, 1, 9, 30, 0, 0, time.UTC)
    if !actual.Equal(expected) {
        t.Errorf("got %v, want %v", actual, expected)
    }
}

func TestStep(t *testing.T) {
    e := mustParse(t, "*/15 * * * *") // every 15 minutes
    actual := e.Next(base)
    expected := time.Date(2024, 1, 1, 0, 15, 0, 0, time.UTC)
    if !actual.Equal(expected) {
        t.Errorf("got %v, want %v", actual, expected)
    }
}

func TestMonthAdvance(t *testing.T) {
    e := mustParse(t, "0 0 1 3 *") // 1st March midnight
    actual := e.Next(base)
    expected := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
    if !actual.Equal(expected) {
        t.Errorf("got %v, want %v", actual, expected)
    }
}

func TestDayOfWeek(t *testing.T) {
    e := mustParse(t, "0 9 * * 1") // 09:00 every Monday
    // base is Monday 2024-01-01
    actual := e.Next(base)
    expected := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
    if !actual.Equal(expected) {
        t.Errorf("got %v, want %v", actual, expected)
    }
}

func TestInvalidField(t *testing.T) {
    cases := []string{
        "60 * * * *",   // minute out of range
        "* 25 * * *",   // hour out of range
        "* * * * 7",    // dow out of range
        "abc * * * *",  // non-numeric
        "* * * *",      // too few fields
    }
    for _, c := range cases {
        if _, err := Parse(c); err == nil {
            t.Errorf("Parse(%q) expected error, got nil", c)
        }
    }
}

func TestConsecutiveNextCalls(t *testing.T) {
    e := mustParse(t, "* * * * *")
    t1 := e.Next(base)
    t2 := e.Next(t1)
    if !t2.Equal(t1.Add(time.Minute)) {
        t.Errorf("consecutive Next: got %v, want %v", t2, t1.Add(time.Minute))
    }
}
