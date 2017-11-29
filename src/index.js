const config = require('config')
const S3FS = require('s3fs')
const path = require('path')
const _ = require('highland')
const gunzip = require('gunzip-maybe')
let timeout = 3000
if (config && config.filesystem && config.filesystem.s3) {
  timeout = config.filesystem.s3.timeout
}
const request = require('request').defaults({ timeout })
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
    let locked
    options = options || {}
    const dir = path.dirname(file)
    const fileName = path.basename(file)
    const params = {
      Bucket: path.join(this.bucket, dir),
      Key: fileName
    }
    const decompress = typeof options.gunzip === 'boolean' && !options.gunzip ? _() : gunzip()
    const url = this.s3.getSignedUrl('getObject', params)
    const output = _()
    request(url)
      .on('error', e => {
        if (!locked) output.emit('error', e)
        locked = true
      })
      .on('response', response => {
        if (response.statusCode !== 200) {
          const error = new Error('Unable to retrieve file')
          error.code = response.statusCode
          if (!locked) output.emit('error', error)
          locked = true
        } else if (response.headers['content-length'] < 1) {
          if (!locked) output.emit('error', new Error('Empty file'))
          locked = true
        }
      })
      .pipe(decompress)
      .on('error', e => {
        if (!locked) output.emit('error', e)
        locked = true
      })
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
    let upload
    let aborted = false

    const pipeline = _.pipeline(stream => {
      const params = this.s3Params(this.bucket, name, options)
      const uploadStream = (params.Body = stream.pipe(zlib.createGzip()))

      upload = this.s3.upload(params, function (err, data) {
        if (err && !aborted) {
          pipeline.emit('error', err)
        } else if (!err) {
          pipeline.emit('finish')
        }
      })
      return uploadStream
    })

    pipeline.end = chunk => {
      if (chunk && !pipeline._nil_pushed) pipeline.write(chunk)
      if (!pipeline._nil_pushed) pipeline.write(_.nil)
    }

    pipeline.abort = function () {
      aborted = true
      upload.abort()
    }

    return pipeline
  }

  /**
   * stat accesses file metadata
   *
   * @param {string} file - filename
   * @param {function} callback
   * @returns {promise} returns resolved promise
   */

  stat (file, callback) {
    const options = {
      Bucket: this.s3Params(this.bucket, file).Bucket,
      Key: this.s3Params(this.bucket, file).Key
    }

    if (callback) {
      this.s3.headObject(options, (err, data) => {
        if (err) {
          err.message = err.name
          return callback(err)
        }
        const statObj = createStatObj(data)
        callback(null, statObj)
      })
    } else {
      return new Promise((resolve, reject) => {
        this.s3.headObject(options, (err, data) => {
          if (err) {
            err.message = err.name
            return reject(err)
          }
          const statObj = createStatObj(data)
          resolve(statObj)
        })
      })
    }
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

function createStatObj (data) {
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
  return statObj
}

Filesystem.type = 'filesystem'
Filesystem.plugin_name = 's3fs'
Filesystem.dependencies = []
Filesystem.version = require('../package.json').version

module.exports = Filesystem
