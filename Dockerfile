FROM golang:1.13 as builder

ENV CGO_ENABLED 0

WORKDIR /go/src/github.com/m-lab/etl-gardener
COPY . .

# Get the requirements and put the produced binaries in /go/bin
# Try twice, because sometimes go get fails for transient reasons
RUN go get -v ./... || go get -v ./...

RUN go install \
      -v \
      -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h)" \
      ./...

FROM alpine
RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

COPY --from=builder /go/bin/gardener /bin/gardener

EXPOSE 9090 8080 8081

WORKDIR /
ENTRYPOINT [ "/bin/gardener" ]
