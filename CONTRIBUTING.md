Overall, the structure of the etcher sdk is fairly simple.

This document is a work in progress, so feedback is appreciated.

## Getting Started

The best place to get started is by looking at the `multi-destination` example in the `examples` directory. This is the quintessential use case for the Balena Etcher, and is useful for understanding the logic and usage of the etcher sdk.

## Code Structure

The structure of the etcher sdk is fairly simple, but can be a bit daunting if you don't know what to look for. Essentially, etcher performs 3 main tasks:

* Scan
* Configure
* Flash

### Scan

TODO: scanner, adapters

### Configure

The most important class you need to remember is `SourceDestination`. This is where the magic happens, and what you need to extend in order to add a device, file, etc. to which the etcher sdk can read/write from.

The class is called `SourceDestination` because it can be either a source or destination to which you can read/write from/to.

If a device can only be read from, you would only implement the `read` functionality, and vice versa for `write`s.

### Flashing

TODO: multiwrite, trimming
