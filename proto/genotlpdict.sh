#!/usr/bin/env bash

# Run this in the top-level directory.
rm -rf ../otlpdict
#mkdir ../otlpdict

# Get current directory.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

for dir in $(find ${DIR}/otlpdict -name '*.proto' -print0 | xargs -0 -n1 dirname | sort | uniq); do
  files=$(find "${dir}" -name '*.proto')

  # Generate all files with protoc-gen-go.
  echo ${files}
  protoc -I ${DIR} --go_out=otlpdict --go-grpc_out=otlpdict ${files}
done

mv otlpdict/github.com/f5/otel-arrow-adapter/otlpdict ../
rm -rf otlpdict/github.com
