maestro
=======

## the sub-projects

Maestro code is layed out in a specific way, so that it has:

 1. We can use _and_ test the macros for generating support from thrift classes
 2. We can have a quickish development lifecycle

This layout is:

 - `core` contains all the base data structures, tasks and utilities that are provided by maestro
   it is not intended that core is used anyway but internal to maestro.
 - `macros` contains macro definitions for fields and codecs, uses core.
 - `api` wraps up and re-exports `core` + `macros` and a reasonable way.
 - `example` an end-to-end usage example of maestro, this is really the easiest way to develop
    maestro. Add what you want and stub out implementation, then work top-down, filling out
    details in core/macros/api
 - `benchmark` is useful for micro-benchmarks, this has been important for working on codecs in
    particular.


## development lifecycle

There is a lot of time wasted doing cluster testing. A round trip can be done in about 2 minutes though
using the following:

 - set-up a mirror of the code on target server (could be local)
 - push code to mirror
 - use `example/bin/update-cluster`, `example/bin/build-cluster` and `example/bin/test-cluster` to run
   through a test loop, most common scenario is to run all three:

```
./maestro-example/bin/update-cluster && ./maestro-example/bin/build-cluster && ./maestro-example/bin/test-cluster
```

Currently these scripts are fairly specific to repository name (it is ~:repo, which is useful for
generic dev on EMR) and work area, but after they see a bit more use, the hope is that the scripts
can become more robust and build out a more resonable dev lifecycle.


## todos

 - Partition.byFields* needs to be cleaned up. The goal is sane error messages, at the moment
   that is only achievable by the non-overloaded, duplicated versions. It might be worth def macro
   a single version and writing a custom error message.

 - Clean-up TemplateParquetSource and move to ebenezer.

 - Add tests around error handling.

 - auto'ify the macro codecs via MacroSupport, requires finding work around for macro-paradise bug on 2.10.3

 - revisit DecodeMacro, and tidy up by trying to pull out for comprehension pattern

 - fix perf of Fields, by using vampire methods trick to get rid of structural type

 - add a CascadeJob base class to work around scalding bug w.r.t. validating cascades
