'use strict'

const test = require('tape')
const config = require('config')
const fs = require('fs')
const request = require('request')
const _ = require('highland')
const zlib = require('zlib')
const parseString = require('xml2js').parseString
const FileSystem = require('../')

test('type and plugin_name are accessible', t => {
  const s3fs = new FileSystem()

  t.equal(s3fs.type, 'filesystem', 'type property returns filesystem')
  t.equal(s3fs.plugin_name, 's3fs', 'plugin_name property returns s3fs')
  t.equal(typeof s3fs.dependencies, typeof [], 'dependencies property returns empty array')
  t.equal(s3fs.version, require('../package.json').version, 'version property properly returns package version')

  t.end()
})

test('CreateWrite stream should write gzipped content correctly', t => {
  const s3fs = new FileSystem()
  const stream = fs.createReadStream('test/fixtures/upload.txt')
    .pipe(s3fs.createWriteStream('upload.txt'))

  stream.on('finish', () => {
    _(request('https://s3.amazonaws.com/test-koop-downloads/upload.txt'))
      .through(zlib.createGunzip())
      .toArray(arr => {
        const txt = arr.join('').toString()
        t.equal(txt, '"Bob is my name"\n',
          'Uploaded and downloaded file should be equal')
        t.end()
      })
  })
})

test('createWriteStream aborts when abort is called', t => {
  const s3fs = new FileSystem()

  const stream = fs.createReadStream('test/fixtures/upload.txt')
    .pipe(s3fs.createWriteStream('uploadAbort.txt'))

  setTimeout(stream.abort.bind(stream), 1)

  s3fs.createReadStream('uploadAbort.txt').toArray(arr => {
    const txt = arr.toString()
    parseString(txt, (err, result) => {
      t.equal(result.Error.Code[0], 'NoSuchKey',
        'createWriteStream successfully aborts')
      t.end()
    })
  })
})

test('createReadStream reads gzipped content', t => {
  const s3fs = new FileSystem()
  const stream = fs.createReadStream('test/fixtures/upload.txt')
    .pipe(s3fs.createWriteStream('readStream.txt'))

  stream.on('finish', () => {
    s3fs.createReadStream('readStream.txt').toArray(arr => {
      const txt = arr.toString()
      t.equal( txt, '"Bob is my name"\n',
        'createReadStream handles gzipped files')
      t.end()
    })
  })
})

test('createReadStream reads non gzipped content', t => {
  const s3fs = new FileSystem()

  s3fs.createReadStream('readStreamNotGzipped.txt').toArray(arr => {
    const txt = arr.toString()
    t.equal(txt, '"Bob is my name"\n',
      'createReadStream handles non gzipped files')
    t.end()
  })
})

test('Metadata is accessible via read/write streams', t => {
  const s3fs = new FileSystem()
  const options = {
    Metadata: {
      test: 'this is a test'
    }
  }
  const stream = fs.createReadStream('test/fixtures/upload.txt')
    .pipe(s3fs.createWriteStream('metadataTest.txt', options))
  stream.on('finish', () => {
    s3fs.stat('metadataTest.txt', (err, data) => {
      t.equal(data.Metadata.test, 'this is a test',
        'Should be able to read metadata object that has been written')
      t.end()
    })
  })
})

test.onFinish(() => {
  const s3fs = new FileSystem()
  const params = {
    Bucket: config.filesystem.s3.bucket,
    Delete: {
      Objects: [
        {
          Key: 'readStream.txt'
        },
        {
          Key: 'upload.txt'
        },
        {
          Key: 'metadataTest.txt'
        }
      ]
    }
  }
  s3fs.s3.deleteObjects(params, (err, data) => {
    console.trace(data)
  })
})
