const config = require('config')
const S3FS = require('s3fs')
const path = require('path')
const _ = require('highland')
const gunzip = require('gunzip-maybe')
const request = require('request').defaults({timeout: 3000})
const zlib = require('zlib')
const Stats = require('s3fs/lib/Stats')

class Filesystem extends S3FS {
  /*
S3FS supports the following methods:
fs.copyDir(sourcePath, destinationPath[, callback])
fs.copyFile(sourcePath, destinationPath[, callback])
fs.create(options[, callback]) - create a base level bucket
fs.delete([callback]) - delete an empty bucket
fs.destroy([callback]) - delete all files in bucket recursively
fs.exists(path, callback)
fs.headObject(path[, callback])
fs.listContents(path, marker[, callback])
fs.lstat(path, callback)
fs.mkdir(path, [mode], callback)
fs.mkdirp(path[, callback])
fs.putBucketLifecycle(name, prefix, days[, callback])
fs.readdir(path, callback)
fs.readdirp(path[, callback])
fs.readFile(filename, [options], callback)
fs.rmdir(path, callback)
fs.rmdirp(path[, callback])
fs.stat(path, callback)
fs.unlink(path, callback)
fs.writeFile(filename, data, [options], callback)
*/

  constructor () {
    super(config.filesystem.s3.bucket, { endpoint: config.filesystem.s3.endpoint })

    this.bucket = config.filesystem.s3.bucket
    if (!this.bucket) throw new Error('No S3 Bucket')

    this.options = config.filesystem.s3.endpoint
  }

  s3Params (bucket, name, options) {
    const dir = path.dirname(name)
    const fileName = path.basename(name)
    options = options || {}
    const params = {
      Bucket: path.join(bucket, dir),
      ContentEncoding: 'gzip',
      Key: fileName,
      ACL: 'public-read',
      Metadata: options.Metadata
    }
    if (options.ContentType) params.ContentType = options.ContentType
    return params
  }

  /**
   * createReadStream reads file from AWS s3 as highland stream
   *
   * @param {string} file - filename
   * @returns {stream}
   */

  createReadStream (file, options) {
    options = options || {}
    const dir = path.dirname(file)
    const fileName = path.basename(file)
    const params = {
      Bucket: path.join(this.bucket, dir),
      Key: fileName
    }
    const decompress = (typeof options.gunzip === 'boolean' && !options.gunzip) ? _() : gunzip()
    const url = this.s3.getSignedUrl('getObject', params)
    const output = _()
    request(url)
    .on('error', function (e) { output.emit('error', e) })
    .on('response', (response) => {
      if (response.headers['content-length'] < 1) {
        output.emit('error', new Error('Empty file'))
        output.destroy()
      }
    })
    .pipe(decompress)
    .on('error', function (e) { output.emit('error', e) })
    .pipe(output)

    return output
  }

  /**
   * createWriteStream writes to AWS S3 as a Highland stream
   *
   * @param {string} name - filename
   * @param {object} options - optional metadata to attach to file
   * @returns {stream} stream
   */

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
    const params = this.s3Params(this.bucket, name, options)
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

  /**
   * stat accesses file metadata
   *
   * @param {string} file - filename
   * @param {function} callback
   * @returns {promise} returns resolved promise
   */

  stat (file, callback) {
    const promise = new Promise((resolve, reject) => {
      this.headObject(file, (err, data) => {
        if (err) {
          err.message = err.name
          return reject(err)
        }
        const statObj = new Stats({
          dev: 0,
          ino: 0,
          mode: 0,
          nlink: 0,
          uid: 0,
          gid: 0,
          rdev: 0,
          size: Number(data.ContentLength),
          atim_msec: data.LastModified,
          mtim_msec: data.LastModified,
          ctim_msec: data.LastModified,
          path: path
        })
        statObj.acceptRanges = data.AcceptRanges
        statObj.ETag = data.ETag
        statObj.ContentType = data.ContentType
        statObj.ContentEncoding = data.ContentEncoding
        statObj.Metadata = data.Metadata
        return resolve(statObj)
      })
    })
    if (!callback) return promise

    promise.then((stats) => {
      callback(null, stats)
    }, (reason) => {
      callback(reason)
    })
  }

  /**
   * Resolve a relative path to a url
   *
   * @param {string} path - a relative path to a file
   * @return {string} the file's url
   */

  realpathSync (path) {
    const endpoint = this.s3.endpoint.href || 'https://s3.amazonaws.com/'
    return `${endpoint}${this.bucket}/${path}`
  }
}
Filesystem.type = 'filesystem'
Filesystem.plugin_name = 's3fs'
Filesystem.dependencies = []
Filesystem.version = require('./package.json').version

module.exports = Filesystem
