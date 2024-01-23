import { ListObjectsV2Command } from "@aws-sdk/client-s3"

/**
 * Returns/creates a test function based on the pattern.
 *  
 * @param {string | RegExp | function | async function} pattern 
 * @returns 
 */
export function _createTest(pattern) {
	let test
	if (pattern) {
		if (typeof pattern === 'function') {
			test = pattern
		}
		else {
			if (typeof pattern === 'string') {
				pattern = new RegExp(pattern)
			}
			if (pattern instanceof RegExp) {
				test = function (value) {
					return pattern.test(value)
				}
			}
		}
	}
	return test
}

/**
 * Finds files and directories a bit like the unix `find` command.
 * 
 * @param {object} options
 * @param {string} options.startingPath The relative path within the sink to begin looking.
 * @param {boolean} options.file Set to true if paths which represent files should be emitted (true by default)
 * @param {boolean} options.directory Set to true if paths which represent directories should be emitted (true by default)
 * @param {string | RegExp | function | async function} options.namePattern A test for the name of the file/directory.
 * If a function it must return true for the path to be emitted. If an async function, it must resolve to true for the
 * path to be emitted. If a regex, the `test` function must return true when passed the name. If a string, it will be
 * passed to `new RegExp()` to create a regular expression.
 * @param {string | RegExp | function | async function} options.pathPattern A test for the path of the file/directory.
 * Works like namePattern except that the relative path value of the item is used instead of just the name.
 * @returns An EventEmitter which emits `data` and `done` events.
 * The `data` events have an object which is the same as returned from getFullFileInfo
 */
export function find({
	file = true
	, directory = true
	, namePattern
	, pathPattern
	, startingPath = ""
} = {}) {
	let output = this._createEventEmitter()

	let nameTest = this._createTest(namePattern)
	let pathTest = this._createTest(pathPattern)

	let combined = this._createCombinedPath(startingPath)
	let self = this


	async function match(name, path, info) {
		if (info.directory && !directory) {
			// If this is a directory and we're not selecting directories
			return
		}
		if (!info.directory && !file) {
			// If this is a file and we're not selecting files, skip
			return
		}
		if (nameTest) {
			let result = nameTest(name)
			if (result instanceof Promise) {
				result = await result
			}
			if (!result) {
				return
			}
		}
		if (pathTest) {
			let result = pathTest(path)
			if (result instanceof Promise) {
				result = await result
			}
			if (!result) {
				return
			}
		}

		output.emit('data', info)
	}

	let command = new ListObjectsV2Command({
		Bucket: this.bucket
		, Prefix: combined
	})


	async function run() {
		try {
			let data = await self.client.send(command)

			while (true) {
				if (!data.Contents) {
					break
				}
				else {
					for (let child of data.Contents) {
						let node = self._makeContentsNode(child)
						match(node.name, node.relPath, node)
					}

					// If there are more entries, let's run it again
					if (data.NextContinuationToken && data.IsTruncated) {
						command = new ListObjectsV2Command({
							Bucket: bucket
							, Prefix: combined
							, ContinuationToken: data.NextContinuationToken
						})

						data = await self.client.send(command)
					}
					else {
						break
					}
				}
			}
			output.emit('done')
		}
		catch (e) {
			output.emit('error', e)
		}
	}

	run()
	return output
}

/**
 * Finds files and directories a bit like the unix `find` command.
 * 
 * @param {object} options See options for `find`.
 * @returns A promise which resolves to an array of strings of relative paths
 * which match the conditions given in the options.
 */
export async function findPaths(options) {
	return new Promise((resolve, reject) => {
		let items = []

		let events = this.find(options)

		events
			.on('data', info => {
				items.push(info.relPath)
			})
			.on('done', () => {
				resolve(items)
			})
			.on('error', (error) => {
				reject(error)
			})
	})
}
