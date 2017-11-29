'use strict'

const test = require('tape')
const config = require('config')
const fs = require('fs')
const zlib = require('zlib')
const request = require('request')
const FileSystem = require('../src')
const nock = require('nock')

const s3fs = new FileSystem()

test('type and plugin_name are accessible', t => {
  t.plan(4)
  t.equal(FileSystem.type, 'filesystem', 'type property returns filesystem')
  t.equal(FileSystem.plugin_name, 's3fs', 'plugin_name property returns s3fs')
  t.equal(typeof FileSystem.dependencies, typeof [], 'dependencies property returns empty array')
  t.equal(FileSystem.version, require('../package.json').version, 'version property properly returns package version')
})

test('CreateWrite stream should write gzipped content correctly', t => {
  t.plan(2)

  fs
    .createReadStream('test/fixtures/upload.txt')
    .pipe(s3fs.createWriteStream('upload.txt'))
    .on('finish', () => {
      request(
        {
          url: 'https://s3.amazonaws.com/test-koop-downloads/upload.txt',
          gzip: true
        },
        (e, response, body) => {
          t.error(e, 'No error getting data')
          t.equal(body, '"Bob is my name"\n', 'Uploaded and downloaded file should be equal')
        }
      )
    })
})

test('CreateWrite stream should accept data in the end call', t => {
  t.plan(2)
  const output = s3fs.createWriteStream('readStream2.txt')

  output.on('finish', () => {
    request(
      {
        url: 'https://s3.amazonaws.com/test-koop-downloads/readStream2.txt',
        gzip: true
      },
      (e, response, body) => {
        t.error(e, 'No error getting data')
        t.equal(body, '"Bob is my name"\nfoobarbaz', 'Uploaded and downloaded file should be equal')
      }
    )
  })

  fs
    .createReadStream('test/fixtures/upload.txt')
    .on('data', d => output.write(d))
    .on('end', () => output.end('foobarbaz'))
})

test('createWriteStream aborts when abort is called', t => {
  t.plan(1)
  const stream = fs.createReadStream('test/fixtures/upload.txt').pipe(s3fs.createWriteStream('uploadAbort.txt'))

  setTimeout(stream.abort.bind(stream), 1)

  request('https://s3.amazonaws.com/test-koop-downloads/test/fixtures/upload.txt', (err, res, body) => {
    if (err) console.trace(err)
    t.equal(res.statusCode, 403, 'file is not on s3')
  })
})

test('createReadStream reads gzipped content', t => {
  t.plan(1)
  const stream = fs.createReadStream('test/fixtures/upload.txt').pipe(s3fs.createWriteStream('readStream.txt'))

  stream.on('finish', () => {
    s3fs.createReadStream('readStream.txt').toArray(arr => {
      const txt = arr.toString()
      t.equal(txt, '"Bob is my name"\n', 'File validates')
    })
  })
})

test('createReadStream does not gunzip gzipped content when gunzip is off', t => {
  t.plan(1)
  const stream = fs.createReadStream('test/fixtures/upload.txt').pipe(s3fs.createWriteStream('readStream.txt'))

  stream.on('finish', () => {
    s3fs
      .createReadStream('readStream.txt', { gunzip: false })
      .through(zlib.createGunzip())
      .toArray(arr => {
        const txt = arr.toString()
        t.equal(txt, '"Bob is my name"\n', 'File validates')
      })
  })
})

test('createReadStream reads non gzipped content', t => {
  t.plan(1)
  s3fs.createReadStream('readStreamNotGzipped.txt').toArray(arr => {
    const txt = arr.toString()
    t.equal(txt, '"Bob is my name"\n', 'File validates')
  })
})

test('Stat includes expected contents', t => {
  t.plan(4)
  const options = {
    ContentType: 'application/json',
    Metadata: {
      test: 'this is a test'
    }
  }
  const stream = fs.createReadStream('test/fixtures/upload.txt').pipe(s3fs.createWriteStream('metadataTest.geojson', options))
  stream.on('finish', () => {
    s3fs.stat('metadataTest.geojson', (err, data) => {
      t.error(err, 'Should be no error')
      t.equal(data.Metadata.test, 'this is a test', 'Should be able to read metadata object that has been written')
      t.equal(data.ContentEncoding, 'gzip', 'Should get the content encoding property')
      t.equal(data.ContentType, 'application/json', 'Should get the content type property')
    })
  })
})

test('Resolve a path to a url', t => {
  t.plan(1)
  const url = s3fs.realpathSync('files/1ef_0/full/1ef_0.geojson')
  t.equal(url, `${config.filesystem.s3.endpoint}/${config.filesystem.s3.bucket}/files/1ef_0/full/1ef_0.geojson`)
})

test('Handle socket timeout', t => {
  t.plan(1)
  nock('https://test-koop-downloads.s3-external-1.amazonaws.com')
    .get(/.+/)
    .socketDelay(2000)
    .reply(200)

  s3fs
    .createReadStream('foo.bar')
    .on('error', e => t.equal(e.message, 'ESOCKETTIMEDOUT'))
    .on('finish', () => t.fail('finish should not fire'))
})

test.onFinish(() => {
  nock.cleanAll()
  const s3fs = new FileSystem()
  const params = {
    Bucket: config.filesystem.s3.bucket,
    Delete: {
      Objects: [
        {
          Key: 'readStream.txt'
        },
        {
          Key: 'readStream2.txt'
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
    if (err) console.trace(err)
  })
})
