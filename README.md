# mnesia_migrate
A tool to upgrade/downgrade schema and migrate data between different versions of mnesia.

[![Build Status](https://travis-ci.org/greyorange/mnesia_migrate.svg?branch=master)](https://travis-ci.org/greyorange/mnesia_migrate)

# Installation

* run `make deps` if you are using erlang.mk

# Usage

* Add contents of priv/sys.config to your application's sys.config or use this config in your release.
* Use forward slash in defining directory names in sys.config
* To use apply_downgrades/1, use a number in the argument which will downgrade that many revisions from currently applied head.
* To enable print statements of library, add {debug, true} in sys.config
* Run `detect_revision_sequence_conflicts()` to get a list of revision id where there is a fork in the revision tree.

# License

MIT License

## Run observability

Every migration attempt is recorded in the `mnesia_migration_runs` Mnesia table
(direction, status `running -> ok | failed`, timestamps, error reason/stacktrace,
node) in addition to the head row in `schema_migrations`.

```erlang
%% Last attempt (record | none)
db_migration:get_last_migration_run().

%% All recorded attempts (list of records)
db_migration:get_run_log().
```

Failures are recorded and reported before the error re-raises, and the head is
only advanced after a successful `up()`, so a retry re-attempts only the
failing revision.

### Custom observer (optional)

Set `{run_log_observer, Module}` under `mnesia_migrate` app env to receive the
same callbacks as the `erl_migrate` `migration_observer` contract
(`on_revision_start/4`, `on_revision_ok/5`, `on_revision_failed/6`,
`on_run_finished/4`), with `schema_name`/`schema_instance` fixed to `legacy`.
Observer failures never affect the migration itself.
