# updates golang dependencies and tidies up the go.mod file
go-update:
    go get -u ./...
    go mod tidy

# tidies up the go.mod file
go-tidy:
    go mod tidy

# run all tests
test target="./...":
    go test -v -cover -coverprofile=coverage.out {{ target }}
    go tool cover -html=coverage.out

# fixes auto-fixable issues (formatting in justfile and golang code)
fix:
    just --format --unstable .
    go fmt ./...

# validate code for issues (formatting in justfile, golang code)
validate:
    test -z $(gofmt -l .)
