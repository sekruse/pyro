#!/bin/bash

basedir="$(cd "$(dirname "$0")/.."; pwd)"

if [ ! "$(pwd)" == "$basedir" ]; then
	>&2 echo "Command must be run from the project root dir ($basedir)."
	exit 1
fi

# Build.
mvn package -DskipTests --offline -P logging,akka-distro || exit 1

# Launch a worker.
java -jar pyro-distro/target/pyro-distro-1.0-SNAPSHOT-akka-distro.jar worker localhost:4243 &
sleep 5

# Do the profiling.
java -jar pyro-distro/target/pyro-distro-1.0-SNAPSHOT-akka-distro.jar profile \
	/Users/basti/Work/Data/csv/OpenAfrica/Special_Primary_Schools_And_Units_-_2016.csv \
	--csv-header --csv-separator comma --csv-quote double --csv-skip-differing-lines \
	--hosts localhost:4242 localhost:4243