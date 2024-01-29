# dbt-server

<p align="center">
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>
<p align="center">

> :warning: **dbt Server is deprecated and will no longer be maintained by dbt Labs going forward. You may continue to use this repository and fork it for your needs. If you would like to be a maintainer of this repository, please reach out to us by opening an issue.**

> dbt Server supported our legacy Semantic Layer, which was [fully deprecated in December 2023.](https://docs.getdbt.com/docs/dbt-versions/release-notes/Dec-2023/legacy-sl). In October 2023, we introduced the GA of the revamped dbt Semantic Layer with [significant improvements](https://www.getdbt.com/blog/build-centralize-and-deliver-consistent-metrics-with-the-dbt-semantic-layer), made possible by the [acquisition of Transform](https://www.getdbt.com/blog/dbt-acquisition-transform) and the integration of [MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow) into dbt. The dbt Semantic Layer is now independent of dbt Server and operates on MetricFlow Server, a new proprietary technology designed for enhanced connectivity and scale.

Welcome to the `dbt-server` repository! The dbt Server is intended to provide a web API for [dbt](https://github.com/dbt-labs/dbt-core) operations and replace the [`dbt-rpc`](https://github.com/dbt-labs/dbt-rpc) Server.

**Important**: this is [major version zero software and APIs should not be considered stable](https://semver.org/#spec-item-4). If you do take a dependency on this software we recommend an exact version pin.

## Understanding dbt Server

dbt is typically used through its command line interface (CLI). The source code of dbt is almost all Python. dbt Server uses [FastAPI](https://github.com/tiangolo/fastapi) to create a web API from a thin wrapper around [`dbt-core`](https://github.com/dbt-labs/dbt-server) and its adapter plugins.

## Getting started

Familiarity with dbt is assumed for dbt Server -- check out how to [get started](https://www.getdbt.com/blog/licensing-dbt/) otherwise.

See the [developer setup in the contributing guide](CONTRIBUTING.md#developer-setup) to get started with dbt Server.

## License

The `dbt-server` repository uses the Business Source License (BSL). That makes the repository "source available", unlike most of our repositories which are "open source". See [the license](LICENSE) and [our blog on licensing](https://www.getdbt.com/blog/licensing-dbt/) for details.

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
