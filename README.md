# Cronjob Implementation (Go)

This is a small Go project for running cron-style jobs from JSON schedule files in `jobs.d/`.

## Project structure

- `main.go` - application entrypoint
- `jobs.d/` - folder containing job definitions (JSON)
  - `daily-report.json`
  - `ronfile.json`
- `go.mod` - Go module definitions

## Requirements

- Go 1.20+ installed

## Run locally

```bash
cd /Users/himanshu/Documents/coding/Projects/Golang/cronjob\ implementation
go run .
```

## Add a job

1. Create a JSON file under `jobs.d/` with scheduling fields.
2. Restart the app.

## Notes

Adjust cron fields and job payloads in the JSON files as needed.
