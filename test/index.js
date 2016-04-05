'use strict'

const test = require('tape')
const fs = require('fs')
const request = require('request')
const _ = require('highland')
const zlib = require('zlib')
const FileSystem = require('../')

test('CreateWrite stream should write gzipped content correctly', (t) => {
  const s3fs = new FileSystem()
  const uploadText = require('./fixtures/upload.txt')
  fs.createReadStream('test/fixtures/upload.txt').pipe(s3fs.createWriteStream('upload.txt'))

  _(request('https://s3.amazonaws.com/koop-dev-bstoltz/upload.txt'))
    .through(zlib.createGunzip())
    .toArray(arr => {
      const txt = arr.join('').toString()

      t.equal(txt, '"Bob is my name"\n',
        'Uploaded and downloaded file should be equal')
    })
  t.end()
})
