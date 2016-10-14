# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [1.2.1] - 2016-10-14
### Changed
* Use Highland@3.0.0

## [1.2.0] - 2016-05-06
### Added
* Option to specify content-type on `createWriteStream`

## [1.1.3] - 2016-04-28
### Changed
* createReadStream will time out after 30 seconds
* createReadStream will emit an error if the content is empty

## [1.1.2] - 2016-04-27
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

[1.2.1]: https://github.com/koopjs/koop-s3fs/compare/v1.2.0..v1.2.1
[1.2.0]: https://github.com/koopjs/koop-s3fs/compare/v1.1.3..v1.2.0
[1.1.3]: https://github.com/koopjs/koop-s3fs/compare/v1.1.2..v1.1.3
[1.1.2]: https://github.com/koopjs/koop-s3fs/compare/v1.1.1..v1.1.2
[1.1.1]: https://github.com/koopjs/koop-s3fs/compare/v1.1.0..v1.1.1
[1.1.0]: https://github.com/koopjs/koop-s3fs/compare/v1.0.2..v1.1.0
[1.0.2]: https://github.com/koopjs/koop-s3fs/compare/v1.0.1..v1.0.2
