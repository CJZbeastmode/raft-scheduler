# pkg/cron

A zero-dependency cron expression parser written in Go. Parses standard 5-field cron expressions and computes the next activation time from any given point.

## Format

```
┌─ minute   (0–59)
│ ┌─ hour   (0–23)
│ │ ┌─ day of month  (1–31)
│ │ │ ┌─ month  (1–12)
│ │ │ │ ┌─ day of week  (0–6, Sun=0)
│ │ │ │ │
* * * * *
```

Each field accepts:

| Syntax | Example | Meaning |
|--------|---------|---------|
| `*` | `*` | every value |
| number | `5` | exact value |
| range | `1-5` | inclusive range |
| step | `*/15` | every Nth value |
| range + step | `0-30/5` | every 5th from 0 to 30 |
| list | `1,15,30` | comma-separated values |

## Usage

```go
import "github.com/YOUR_USERNAME/raft-scheduler/pkg/cron"

e, err := cron.Parse("30 9 * * 1")
if err != nil {
    log.Fatal(err)
}

next := e.Next(time.Now())
fmt.Println(next) // next Monday at 09:30
```

Calling `Next()` repeatedly gives successive activation times:

```go
t := time.Now()
for i := 0; i < 5; i++ {
    t = e.Next(t)
    fmt.Println(t)
}
```

## Examples

```
"* * * * *"        every minute
"0 9 * * 1"        9:00am every Monday
"*/15 * * * *"     every 15 minutes
"30 8 * * 1-5"     8:30am weekdays only
"0 0 1 * *"        midnight on the 1st of every month
"0 0 31 2 *"       impossible — Next() returns time.Time{} (zero value)
```

## API

### `Parse(expr string) (*Expression, error)`

Parses a 5-field cron expression. Returns an error if any field is invalid or out of range.

### `(*Expression) Next(t time.Time) time.Time`

Returns the next activation time strictly after `t`. Truncates to minute precision. Returns the zero `time.Time` if no match is found within 4 years (e.g. an impossible expression).

## Day matching semantics

Follows standard vixie-cron OR semantics: if both day-of-month and day-of-week are restricted, a day is valid if **either** matches. If both are `*`, every day is valid.

```
"0 9 1 * 1"  →  9am on the 1st of the month, OR 9am every Monday
```

## Implementation

Each parsed field is stored as a `uint64` bitmask — one bit per valid value. Checking whether a value matches is a single bitwise AND operation. `Next()` walks forward in time checking each field against its bitmask, advancing to the next valid value at each level.

## Testing

```bash
go test ./pkg/cron/... -v -race
```

All 7 tests pass including consecutive calls, step expressions, month advancement, day-of-week matching, and invalid field detection.