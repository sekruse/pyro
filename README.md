# Pyro

Pyro is a data profiling algorithm to detect approximate/partial keys and functional dependencies in relational datasets.
It is compatible with [Metanome](https://github.com/HPI-Information-Systems/Metanome) and [Metacrate](https://github.com/stratosphere/metadata-ms).
Furthermore, it supports [Akka](https://akka.io/) to run on computer clusters.

Please find usage and installation instructions below.

## Installation

Before installing Pyro, you will need to build and install (via `mvn install`) some of its dependencies.

Required:
* [Metanome CLI](https://github.com/sekruse/metanome-cli)

Optional (these dependencies should be available from Snapshot repositories):
* [Metanome](https://github.com/HPI-Information-Systems/Metanome)
* [Metacrate](https://github.com/stratosphere/metadata-ms)

Eventually, you can build Pyro via `mvn clean package -Pdistro`.
You should then find the file `pyro-distro-1.0-SNAPSHOT-distro.jar` (or similar) in the `pyro-distro/target` folder.
This is your deployable Pyro distribution.

## Usage

**Usage with Metanome CLI.**
Just follow the [usage instructions](https://github.com/sekruse/metanome-cli#usage) of the Metanome CLI along with your Pyro distribution jar.
The distribution in fact contains multiple algorithms:
* `de.hpi.isg.pyro.algorithms.Pyro`: Single-node version of Pyro.
* `de.hpi.isg.pyro.metanome.algorithms.PyroAkka`: Distributed version of Pyro (see below).
* `de.hpi.isg.pyro.algorithms.TaneX`: Implementation of the Tane algorithm, adapted for additional key discovery.
* `de.hpi.isg.pyro.algorithms.ADuccDfd`: A joint implementation of the Ducc and Dfd algorithms, adapted for approximate/partial dependency discovery.
* `de.hpi.isg.pyro.algorithms.FdepX`: Implementation of the Fdep algorithm, including approximate/partial dependency discovery. The latter version does not always yield correct results, though, due to a logical error in the algorithm.
To learn more about the parameters of those algorithms, have a look at the class `de.hpi.isg.pyro.core.AbstractPFDConfiguration` and its subclasses.

**Usage with Metanome (untested).**
Load the algorithm into Metanome and configure its properties as described above.

**Distributed execution.**
To execute Pyro in the distributed mode, you need to launch Pyro workers on each cluster machine first.
To do so, copy the Pyro distribution jar file to each machine and execute the contained main class `de.hpi.isg.pyro.akka.algorithms.Pyro` with the `worker` command, e.g.,
```
java -cp pyro-distro-1.0-SNAPSHOT-distro.jar:... de.hpi.isg.pyro.akka.algorithms.Pyro worker <options>...
```
Then, you can either run the Metanome algorithm `de.hpi.isg.pyro.metanome.algorithms.PyroAkka` from Metanome or Metanome CLI or alternatively execute `de.hpi.isg.pyro.akka.algorithms.Pyro` with the `profile` command, e.g.,
```
java -cp pyro-distro-1.0-SNAPSHOT-distro.jar:... de.hpi.isg.pyro.akka.algorithms.Pyro profile <options>...
```
In any case, you will need to specify the workers you have started before, as the latter two commands start a master that only instructs the worker with profiling tasks and collects their results.

Caveat: As of writing this text, Metanome does not support approximate/partial dependencies. To work around this issue, Pyro passes its discovered dependencies to Metanome as normal, exact dependencies, i.e., it scraps the error from the discovered approximate/partial dependencies. If you need the calculated error (or even an assessment of the dependency score), you can need to write the results directly to Metacrate, in which case Pyro should output actual approximate/partial dependencies.
You can do so by running Pyro with Metanome CLI and specify `crate:my-metacrate-instance.db` (or similar) as output.
