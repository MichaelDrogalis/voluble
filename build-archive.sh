#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

lein jar

VERSION_LOCATION="/tmp/version.txt"
if [ -d "$VERSION_LOCATION" ]; then rm -rf $VERSION_LOCATION; fi
lein with-profile package project-version > $VERSION_LOCATION

VERSION=$(<$VERSION_LOCATION)
BUILD_LOCATION="target/archive/mdrogalis-voluble-$VERSION"

if [ -d "$BUILD_LOCATION" ]; then rm -rf $BUILD_LOCATION; fi

mkdir -p $BUILD_LOCATION/doc
mkdir -p $BUILD_LOCATION/lib

JAR_LOCATION="target/voluble-$VERSION.jar"
LIB_LOCATION="lib"

if [ -d "$LIB_LOCATION" ]; then rm -rf $LIB_LOCATION; fi

lein libdir
cp $LIB_LOCATION/* $BUILD_LOCATION/lib/

lein with-profile package run -- "$BUILD_LOCATION/manifest.json"

cp $JAR_LOCATION "$BUILD_LOCATION/lib/"

cp LICENSE "$BUILD_LOCATION/doc/"
cp README.md "$BUILD_LOCATION/doc/"

ARCHIVE_FILE="mdrogalis-voluble-$VERSION.zip"
ARCHIVE_LOCATION="target/$ARCHIVE_FILE"
if [ -d "$ARCHIVE_LOCATION" ]; then rm -rf $ARCHIVE_LOCATION; fi

(cd $BUILD_LOCATION/.. && zip -r "../$ARCHIVE_FILE" .)
