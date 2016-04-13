# koop-s3fs
[![Build Status](https://travis-ci.org/koopjs/koop-s3fs.svg?branch=master)](https://travis-ci.org/koopjs/koop-s3fs)


Koop integration with Amazon S3

## Install

koop-s3fs should be installed as a dependency in a Node.js project like so:

```
npm install koop-s3fs --save
```

## Config

koop-s3fs requires a json config file like so:

```
{
  "filesystem": {
    "s3": {
      "bucket": $S3_BUCKET,
      "endpoint" $S3-ENDPOINT //optional https://forums.aws.amazon.com/ann.jspa?annID=3112
    }
  }
}
```

Additionally you will need to supply your:

```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```
or configure the AWS CLI with keys.


## Methods

```js
const Filesystem = require('koop-s3fs')
const s3fs = new Filesystem()
const fs = require('fs')

/**
 * createWriteStream writes to AWS S3 as a Highland stream
 *
 * @param {string} name - filename
 * @param {object} options - optional metadata to attach to file
 * @returns {stream} stream
 */

const stream = fs.createReadStream('path/to/file')
  .pipe(s3fs.createWriteStream('filename'))


/**
 * createReadStream reads file from AWS s3 as highland stream
 *
 * @param {string} file - filename
 * @returns {stream}
 */

s3fs.createReadStream('filename').toArray(arr => {
  const txt = arr.toString()
  console.log(txt)
})


/**
 * stat accesses file metadata
 *
 * @param {string} file - filename
 * @param {function} callback
 * @returns {promise} returns resolved promise
 */

s3fs.stat('filename', (err, data) => {
  console.log(data)
  /*  Stats {
      getEntry: [Function],
      dev: 0,
      mode: 0,
      nlink: 0,
      uid: 0,
      gid: 0,
      rdev: 0,
      blksize: undefined,
      ino: 0,
      size: 37,
      blocks: undefined,
      atime: Wed Apr 13 2016 13:15:14 GMT-0400 (EDT),
      mtime: Wed Apr 13 2016 13:15:14 GMT-0400 (EDT),
      ctime: Wed Apr 13 2016 13:15:14 GMT-0400 (EDT),
      birthtime: Invalid Date,
      acceptRanges: 'bytes',
      ETag: '"b3b285a1749957f3e3a627a8e92b65bc"',
      ContentType: 'application/octet-stream',
      Metadata: { test: 'this is a test' } }
  */
})
```
