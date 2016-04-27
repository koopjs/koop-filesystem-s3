# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased
### Added
* Option to disable automatic gunzipping of `createReadStream` content with `{gunzip: false}`

## [1.1.1] - 2016-04-27
### Changed
* Stat object includes content encoding

## [1.1.0] - 2016-04-16
### Added
* `realPathSync` returns url location when given relative path
* Set `Content-Encoding` header on saved files

## [1.0.2] - 2016-04-13
### Changed
* Type, plugin_name, dependencies, version accessible from class object

[1.1.1]: https://github.com/koopjs/koop-s3fs/compare/v1.1.0..v1.1.1
[1.1.0]: https://github.com/koopjs/koop-s3fs/compare/v1.0.2..v1.1.0
[1.0.2]: https://github.com/koopjs/koop-s3fs/compare/v1.0.1..v1.0.2
