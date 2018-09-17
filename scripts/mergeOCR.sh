#!/usr/bin/env bash

# merge OCR text files

ocrtxt="$1"
shift

cat "$@" > "$ocrtxt"

exit 0
