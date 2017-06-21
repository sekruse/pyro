#!/bin/bash

basedir="$(cd "$(dirname "$0")/.."; pwd)"

if [ ! "$(pwd)" == "$basedir" ]; then
	>&2 echo "Command must be run from the project root dir ($basedir)."
	exit 1
fi

# Build.
mvn package -DskipTests -P logging,akka-distro -pl '!:pyro-metanome' "$@"

