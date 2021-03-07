############################################################################
# FINAL IMAGE: based on upstream gcloud builder.
FROM gcr.io/cloud-builders/gcloud AS gardener-testing

# Fetch recent go version.
ENV GOLANG_VERSION 1.15.8
ENV GOLANG_DOWNLOAD_URL https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 d3379c32a90fdf9382166f8f48034c459a8cc433730bc9476d39d9082c94583b

RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
    && echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - \
    && tar -C /usr/local/ -xzf golang.tar.gz \
    && rm golang.tar.gz

ENV PATH /usr/local/go/bin:$PATH
ENV GOPATH /go
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
WORKDIR $GOPATH

RUN apt-get update
RUN apt-get install -y jq gcc
RUN go get -v github.com/m-lab/gcp-config/cmd/cbif

ENV CGO_ENABLED 0
# Copy sources into image before build.
WORKDIR /go/src/github.com/m-lab/etl-gardener
COPY . .

# Get the requirements and put the produced binaries in /go/bin
RUN go get -v ./...
RUN go install \
      -v \
      -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h) -X main.Version=$(git describe --tags) -X main.GitCommit=$(git log -1 --format=%H)" \
      ./cmd/gardener

WORKDIR $GOPATH
ENTRYPOINT ["/go/bin/cbif"]

FROM alpine:3.12
RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

COPY --from=gardener-testing /go/bin/gardener /bin/gardener

EXPOSE 9090 8080 8081

WORKDIR /
ENTRYPOINT [ "/bin/gardener" ]
