#!/usr/bin/env bash

# generate OCR from srcimg into outdir/ocrtxt

srcimg="$1"
outdir="$2"
ocrtxt="$3"

scriptname="$(basename "$0")"
tmpbase="${ocrtxt}.$$.${RANDOM}"

function msg ()
{
	echo "[$scriptname] $@"
}

function die ()
{
	msg "$@"
	exit 1
}

function generate_ocr
{
	# straightforward approach

	tmpimg="${tmpbase}.tif"
	tmptxt="${tmpbase}.txt"

	convert -density 300 -units PixelsPerInch -type Grayscale +compress "$srcimg" "$tmpimg" || die "failed to convert image: [$srcimg]"

	tesseract "$tmpimg" "$tmpbase" --psm 1 --oem 2 || die "failed to OCR image: [$tmpimg]"

	[ ! -f "$tmptxt" ] && die "cannot find OCR text file: [$tmptxt]"

	mv -f "$tmptxt" "$ocrtxt"

	rm -f "$tmpimg"
}

[ ! -f "$srcimg" ] && die "could not find image file: [$srcimg]"

cd "$outdir" || die "could not change to output directory: [$outdir]"

generate_ocr

exit 0
