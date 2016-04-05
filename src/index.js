const config = require('config')
const S3FS = require('s3fs')
const path = require('path')
const _ = require('highland')
const gunzip = require('gunzip-maybe')
const request = require('request')
const zlib = require('zlib')

module.exports = class Filesystem extends S3FS {
  constructor () {
    super(config.filesystem.s3.bucket, { endpoint: config.filesystem.s3.endpoint })
    this.type = 'filesystem'
    this.plugin_name = 's3fs'

    this.bucket = config.filesystem.s3.bucket
    if (!this.bucket) throw new Error('No S3 Bucket')

    this.options = config.filesystem.s3.endpoint
    this.dependencies = []
    this.version = require('./package.json').version
  }

  s3Params (bucket, name, options) {
    const dir = path.dirname(name)
    const fileName = path.basename(name)
    options = options || {}
    return {
      Bucket: path.join(bucket, dir),
      Key: fileName,
      ACL: 'public-read',
      Metadata: options.metadata
    }
  }

  createReadStream (file) {
    const dir = path.dirname(file)
    const fileName = path.basename(file)
    const params = {
      Bucket: path.join(this.s3Bucket, dir),
      Key: fileName
    }
    const url = this.s3.getSignedUrl('getObject', params)
    let output = _()
    request(url)
    .on('error', function (e) { output.emit('error', e) })
    .pipe(gunzip())
    .on('error', function (e) { output.emit('error', e) })
    .pipe(output)

    return output
  }

  createWriteStream (name, options) {
    let aborted = false
    const input = _()
    const through = _()
    input.on('data', (chunk) => {
      through.write(chunk)
    })
    input.end = (chunk) => {
      if (chunk) through.write(chunk)
      through.write(_.nil)
    }
    const params = this.s3Params(this.bucket, name, this.options)
    params.Body = through.pipe(zlib.createGzip())

    const upload = this.s3.upload(params, (err, data) => {
      if (err && !aborted) input.emit('error', err)
      else if (!err) input.emit('finish')
      input.destroy()
    })

    input.abort = function () {
      aborted = true
      upload.abort()
    }

    return input
  }

}
