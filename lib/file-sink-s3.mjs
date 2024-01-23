
import EventEmitter from "events"
import {
	S3Client, PutObjectCommand, DeleteObjectsCommand, GetObjectCommand, ListObjectsV2Command
} from "@aws-sdk/client-s3"
import addCallbackToPromise from 'add-callback-to-promise'
import pathTools from 'path'
import { PassThrough } from 'stream'
import { _createTest, find, findPaths } from './find-methods.mjs'
import { createHash } from "crypto"

export default class FileSinkS3 {
	constructor({ bucket, prefix = '', connection}) {
		// super(prefix)
		this.bucket = bucket
		this.connection = connection
		this.prefix = prefix

		this.client = new S3Client(connection)
	}

	isAllowedPath(path) {
		if (path.indexOf('..') > -1) {
			return false
		}
		return true
	}

	_createCombinedPath(path) {
		if (path.startsWith('/')) {
			path = path.substring(1)
		}
		return this.prefix + path
	}
	_convertToBinary(data) {
		if (typeof data == 'string') {
			return Buffer.from(data)
		}

		return data
	}

	_getFirstFunction(args) {
		let newArgs = [...args]

		for (let i = 2; i < newArgs.length; i++) {
			let item = newArgs[i]
			if (typeof item == 'function') {
				return item
			}
		}
		return null
	}

	_getOptions(args) {
		let newArgs = [...args]

		for (let i = 2; i < newArgs.length; i++) {
			let item = newArgs[i]
			if (typeof item == 'object') {
				return item
			}
		}
		return null
	}

	/**
	 * Okay, seems a little trivial, but I want to be able to use the find code
	 * with other file-sink compatible storage systems on the browser for which
	 * I do NOT want to drag the entire EventEmitter code.
	 * @returns An EventEmitter
	 */
	_createEventEmitter() {
		return new EventEmitter()
	}


	/**
	 * Reads data from a file
	 * @param {string} path The path of the file within the sink
	 * @param {function} [callback] An optional callback. If specified, 
	 * it will be added to the promise chain.
	 * @returns A promise which resolves to data from the file, a Buffer
	 */
	read(path, callback) {
		if (!this.isAllowedPath(path)) {
			throw new Error('Path now allowed: ' + path)
		}
		let combined = this._createCombinedPath(path)

		let p = new Promise(async (resolve, reject) => {
			try {
				let command = new GetObjectCommand({
					Bucket: this.bucket
					, Key: combined
				})
				let data = await this.client.send(command)
				let buffer = await data.Body.transformToByteArray()
				// to make it a string, do like below
				// let enc = new TextDecoder("utf-8")
				resolve(Buffer.from(buffer))
			}
			catch (e) {
				reject(e)
			}
		})

		addCallbackToPromise(p, callback)
		return p
	}

	/**
	 * Reads data from a file. Available for interface compatibility, but it just
	 * throws an error because data is not available synchronously.
	 * 
	 * @param {string} path The path of the file within the sink
	 * @returns A Buffer with the file data
	 */
	readSync(path) {
		throw new Error('This sink must read data asynchronously')
	}

	createHash(path, algorithm = 'sha512') {
		let p = new Promise((resolve, reject) => {
			try {
				const hash = createHash(algorithm)
				const stream = this.readStream(path)
				stream.on('error', err => reject(err))
				stream.on('data', chunk => {
					hash.update(chunk)
				})
				stream.on('end', () => resolve(hash.digest('hex')))
			}
			catch(e) {
				reject(e)
			}
		})
		return p
	}



	/**
	 * Creates a file read stream
	 * 
	 * @param {string} path The path of the file within the sink
	 * @returns An stream object
	 */
	readStream(path) {
		if (!this.isAllowedPath(path)) {
			throw new Error('Path now allowed: ' + path)
		}
		let pass = new PassThrough()
		let p = new Promise(async (resolve, reject) => {
			try {
				let data = await this.read(path)

				pass.end(data, () => {
				})
			}
			catch (e) {
				pass.emit('error', e)
			}
		})
		return pass
	}

	/**
	 * Writes data to a file.
	 * 
	 * 
	 * @param {string} path 
	 * @param {string | Buffer | TypedArray | DataView} data 
	 * @param {object} options Options, including those normally associated with fs.writeFile
	 * @param {object} options.offset Offset into the data
	 * @param {object} options.length The length of the data to write
	 * @param {object} options.position Position within the file
	 * @param {function} [callback] An optional callback. If specified, 
	 * it will be added to the promise chain.
	 * @returns A promoise which resolves null
	 */
	async write(path, data) {
		if (!this.isAllowedPath(path)) {
			throw new Error('Path now allowed: ' + path)
		}
		let combined = this._createCombinedPath(path)
		let callback = this._getFirstFunction(arguments)
		let options = this._getOptions(arguments) || {}
		let sendData

		if (options.position) {
			// Ugg. We've got to fetch the entire thing so we can do a partial update
			// headers.Position = options.position
			throw new Error('not yet supported')
		}
		else {
			sendData = this._convertToBinary(data)
			if ((options.length || options.offset) && Buffer.isBuffer(sendData)) {
				sendData = Buffer.from(sendData, options.offset || 0, options.length)
			}

		}


		let command = new PutObjectCommand({
			Bucket: this.bucket
			, Key: combined
			, Body: sendData
		})

		let p = this.client.send(command)
		return addCallbackToPromise(p, callback)
	}
	
	_removeSlashes(path) {
		while(path.startsWith('/')) {
			path = path.substring(1)
		}
		while(path.endsWith('/')) {
			path = path.substring(0, path.length - 1)
		}

		return path
	}


	_makeContentsNode(content, parentPath) {
		let endsWithSlash = content.Key.endsWith('/')
		let name = pathTools.basename(content.Key) 

		let n = {
			name: name,
			parent: pathTools.dirname(content.Key),
			stat: {
				size: content.Size
				, mtime: content.LastModified
				, mtimeMs: new Date(content.LastModified).getTime()
			},
			directory: false
		}
		if(parentPath) {
			n.relPath = pathTools.join(parentPath, name)
		}
		else {
			let relPath = pathTools.join(n.parent, n.name)
			relPath = relPath.substring(this.prefix.length)
			n.relPath = relPath
		}
		
		n.relPath = this._removeSlashes(n.relPath)

		if(endsWithSlash && n.stat.size == 0) {
			n.directory = true
		}

		if (n.parent == '.') {
			n.parent = ''
		}
		this._addAccessUrl(n)
		return n
	}

	_makeCommonPrefixNode(prefix, parentPath) {
		let name = pathTools.basename(prefix.Prefix)
		let n = {
			name: name,
			parent: pathTools.dirname(prefix.Prefix),
			stat: {},
			directory: true,
			relPath: this._removeSlashes(pathTools.join(parentPath, name))
		}
		this._addAccessUrl(n)
		return n
	}
	
	_addAccessUrl(node) {
		node.accessUrl = this._determineAccessUrl(pathTools.join(node.parent, node.name))
		return node
	}

	_determineAccessUrl(combinedPath) {
		let accessUrl = `https://${this.bucket}.s3.amazonaws.com/${combinedPath}`
		while (accessUrl.endsWith('/')) {
			accessUrl = accessUrl.substring(0, accessUrl.length - 1)
		}
		return accessUrl
	}

	/**
	 * Get the details for a file, including the children if the path points to
	 * a directory.
	 */
	async getFullFileInfo(path, callback) {
		if (!this.isAllowedPath(path)) {
			throw new Error('Path now allowed: ' + path)
		}
		let combined = this._createCombinedPath(path)

		let command = new ListObjectsV2Command({
			Bucket: this.bucket
			, Delimiter: '/'
			, Prefix: combined
		})


		let p = new Promise(async (resolve, reject) => {
			try {
				let data = await this.client.send(command)
				let info

				if (data.Contents && data.Contents.length == 1 && data.Contents[0].Key == combined) {
					info = this._makeContentsNode(data.Contents[0], pathTools.dirname(path))
				}
				else if (!data.Contents && !data.CommonPrefixes) {
					reject(new Error('Path not found.'))
				}
				else {
					if (path.endsWith('/') == false 
						&& data.CommonPrefixes 
						&& data.CommonPrefixes.length == 1 
						&& data.CommonPrefixes[0].Prefix == combined + '/') {
						// It probably means this is a directory and we need to query with the slash or we'll just get the directory entry
						
						info = await this.getFullFileInfo(path + '/')
						resolve(info)
					}
					info = {
						name: pathTools.basename(combined),
						parent: pathTools.dirname(combined),
						stat: {},
						directory: true,
						relPath: this._removeSlashes(path),
						children: []
					}
					this._addAccessUrl(info)

					while (true) {
						if (data.Contents) {
							for (let child of data.Contents) {
								if(child.Key == combined) {
									continue
								}
								info.children.push(this._makeContentsNode(child, path))
							}
						}
						if (data.CommonPrefixes) {
							for (let prefix of data.CommonPrefixes) {
								info.children.push(this._makeCommonPrefixNode(prefix, combined))
							}
						}

						// If there are more entries, let's run it again
						if (data.NextContinuationToken && data.IsTruncated) {
							command = new ListObjectsV2Command({
								Bucket: bucket
								, Prefix: combined
								, Delimiter: '/'
								, ContinuationToken: data.NextContinuationToken
							})

							data = await this.client.send(command)
						}
						else {
							break
						}

					}
				}

				resolve(info)
			}
			catch (e) {
				reject(e)
			}
		})
		return addCallbackToPromise(p, callback)
	}


	/**
	 * Removes a file or directory
	 * @param {string} path 
	 * @param {function} [callback]
	 * @param {object} [options]
	 * @param {object} [options.recursive] If true will delete a directory and its contents (true by default)
	 * @returns 
	 */
	async rm(path, callback, options) {
		if (!this.isAllowedPath(path)) {
			throw new Error('Path now allowed: ' + path)
		}
		let combined = this._createCombinedPath(path)
		if (typeof callback == 'object' && !options) {
			options = callback
			callback = null
		}

		options = Object.assign({
			recursive: true
		}, options)


		let p = new Promise(async (resolve, reject) => {
			try {
				let keys
				if (!options.recursive) {
					keys = [
						combined
					]
				}
				else {
					let command = new ListObjectsV2Command({
						Bucket: this.bucket
						, Prefix: combined
					})
					let data = await this.client.send(command)
					if (data.Contents && data.Contents.length > 0) {
						keys = data.Contents.map(content => content.Key)
					}
					else {
						// no matches, so we need signal an error
						return reject(new Error('No matching files found to delete.'))
					}
				}
				let result = []
				while (keys.length > 0) {
					let batchSize = Math.min(keys.length, 1000)
					let batch = keys.splice(0, batchSize)
					let command = new DeleteObjectsCommand({
						Bucket: this.bucket,
						Delete: {
							Objects: batch.map(key => ({ Key: key }))
						}
					})

					let response = await this.client.send(command)
					result.push(response)
					if (!response.Deleted || response.Deleted.length == 0) {
						// We didn't delete any objects, which is an error
						return reject(response)
					}
				}
				return resolve(result)
			}
			catch (e) {
				reject(e)
			}
		})
		return addCallbackToPromise(p, callback)
	}

	/**
	 * Makes a directory
	 * @param {string} path 
	 * 
	 * @returns a promise which resolves to the fs.promises.mkdir promise resolution
	 */
	mkdir(path) {
		if (!this.isAllowedPath(path)) {
			throw new Error('Path now allowed: ' + path)
		}
		if (!path.endsWith('/')) {
			path += '/'
		}

		return this.write(path, '')
	}

	_createTest = _createTest
	find = find
	findPaths = findPaths


}