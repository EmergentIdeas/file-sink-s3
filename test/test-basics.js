import mocha from "mocha"
import { assert } from 'chai'

import {
	S3Client, ListBucketsCommand, PutObjectCommand,
	DeleteObjectCommand, GetObjectCommand, ListObjectsV2Command
} from "@aws-sdk/client-s3"
import credentials from '../credentials/test-credentials.mjs'
import FileSinkS3 from "../lib/file-sink-s3.mjs"

let bucket = 'file-sink-s3-test-1'
let fileKey = 'f' + (new Date()).getTime()
function getSink() {
	return new FileSinkS3({
		bucket: bucket
		, prefix: fileKey + '/'
		, connection: credentials
	})
}

let time = new Date().getTime()
let msg = 'this is a test: ' + time

describe("a basic test of the aws toolkit as file-sink", function () {


	it("a simple write", function (done) {
		let s = getSink()
		try {
			s.write('data1.txt', msg, function (error) {
				if (error) {
					return done(error)
				}
				return done()
			})
		}
		catch (e) {
			done(e)
		}
	})
	it("size of write", async function () {
		let s = getSink()
		let info = await s.getFullFileInfo('data1.txt')
		assert.equal(info.stat.size, msg.length)
	})
	it("stream read", function (done) {
		let s = getSink()
		let stream = s.readStream('data1.txt')
		stream.on('error', err => done(err))
		stream.on('data', chunk => {let i = 1})
		stream.on('end', () => done())
	})

	it("test remove", function (done) {
		let filename = 'data3.txt'
		let s = getSink()
		let f = async () => {
			try {
				await s.write(filename, '')
				let info = await s.getFullFileInfo(filename)
				assert.equal(info.stat.size, 0)

				await s.rm(filename)

				try {
					info = await s.getFullFileInfo(filename)
					done(new Error(info))
				}
				catch (e) {
					// the file does not exist, so that should get an error
					done()
				}
			}
			catch (err) {
				done(new Error(err))
			}
		}
		f()
	})
	
	it("a positional write", function (done) {
		let s = getSink()
		let f = async () => {
			try {

				await s.write('data2.txt', '')
				await s.write('data2.txt', 'b', {
					position: 10
				})
				await s.write('data2.txt', 'a', {
					position: 1
				})

				let data = await s.read('data2.txt')
				assert.equal(data.length, 11, "Expected length to be 11")
				assert.equal(data.slice(1, 2).toString(), 'a')
				assert.equal(data.slice(10, 11).toString(), 'b')
				done(new Error("We should have gotten a not supported error"))
			}
			catch (err) {
				done()
			}
		}
		f()
	})

	it("a positional write of buffers", function (done) {
		let s = getSink()
		let f = async () => {
			try {

				await s.write('data2.txt', '')
				await s.write('data2.txt', Buffer.alloc(20, 2), {
				})

				let data = await s.read('data2.txt')
				assert.equal(data.length, 20, "Expected length to be 20")
				for (let i = 0; i < 20; i++) {
					assert.equal(data[i], 2)
				}
				done()
			}
			catch (err) {
				done(new Error('Could not write buffers for some reason'))
			}
		}
		f()
	})
	it("create hash", function (done) {
		let s = getSink()
		let filename = 'data1.txt'
		let f = async () => {
			try {
				let hash = await s.createHash(filename)
				console.log(hash)

				done()
			}
			catch (err) {
				done(new Error(err))
			}
		}
		f()
	})

	it("test remove, non-recursive", function (done) {
		let filename = 'data3.txt'
		let s = getSink()
		let f = async () => {
			try {
				await s.write(filename, '')
				let info = await s.getFullFileInfo(filename)
				assert.equal(info.stat.size, 0)

				await s.rm(filename, {
					recursive: false
				})

				try {
					info = await s.getFullFileInfo(filename)
					done(new Error(info))
				}
				catch (e) {
					// the file does not exist, so that should get an error
					done()
				}
			}
			catch (err) {
				done(new Error(err))
			}
		}
		f()
	})

	it("a simple write promise", function (done) {
		let s = getSink()
		s.write('data1.txt', msg).then(() => {
			done()
		}).catch(error => {
			return done(error)
		})

	})

	it("a simple write await", function (done) {
		let s = getSink()
		let f = async () => {
			try {
				let result = await s.write('data1.txt', msg)
				done()
			}
			catch (err) {
				done(new Error(err))
			}
		}
		f()
	})

	it("a simple read", function (done) {
		let s = getSink()
		s.read('data1.txt', function (error, data) {
			if (error) {
				return done(error)
			}
			if (msg == data.toString()) {
				done()
			}
			else {
				done(new Error('contents read did not match contents written'))
			}
		})

	})

	it("a simple promise read", function (done) {
		let s = getSink()
		s.read('data1.txt').then(data => {
			if (msg == data.toString()) {
				done()
			}
			else {
				done(new Error('contents read did not match contents written'))
			}
		}).catch(err => {
			done(new Error(err))
		})
	})

	it("a simple promise read failure", function (done) {
		let s = getSink()
		s.read('data1-does-not-exist.txt').then(data => {
			done(new Error('this file should not exist'))
		}).catch(err => {
			done()
		})
	})

	it("a simple promise await read", function (done) {
		let f = async () => {
			let s = getSink()
			try {
				let data = await s.read('data1.txt')
				if (msg == data.toString()) {
					done()
				}
				else {
					done(new Error('contents read did not match contents written'))
				}
			}
			catch (err) {
				done(new Error(err))
			}
		}

		f()
	})

	it("a simple promise await read failure", function (done) {
		let f = async () => {
			let s = getSink()
			try {
				let data = await s.read('data1-does-not-exist.txt')
				if (msg == data.toString()) {
					done(new Error('file should not exist'))
				}
				else {
					done(new Error('contents read did not match contents written'))
				}
			}
			catch (err) {
				// This is correct since the file shouldn't exist
				done()
			}
		}

		f()
	})

	it("a stream read", function (done) {
		let s = getSink()
		try {
			let data = ''
			let stream = s.readStream('data1.txt')
			stream.on('data', (chunk) => {
				data += chunk
			})
			stream.on('close', () => {
				if (msg == data.toString()) {
					done()
				}
				else {
					done(new Error('contents read did not match contents written'))
				}
			})
		}
		catch (error) {
			return done(error)
		}
	})

	it("a stream read of a non-existent file", function (done) {
		let s = getSink()
		let triggeredError = false
		try {
			let stream = s.readStream('data1-this-does-not-exist.txt')
			stream.on('data', (chunk) => {
				if (!triggeredError) {
					done(new Error('should not have gotten data'))
					triggeredError = true
				}
			})
			stream.on('close', () => {
				if (!triggeredError) {
					done(new Error('should not have gotten data'))
					triggeredError = true
				}
			})
			stream.on('error', (err) => {
				// we got an error, which is good, since this file does not exist
				done()
			})
		}
		catch (error) {
			return done(error)
		}
	})

	it("delete non-existent file", function (done) {
		let s = getSink()
		try {

			let promise = s.rm('testfile4')
			promise.then((data) => {
				done(new Error('we should have gotten an error'))
			})
				.catch(err => {
					done()
				})
		}
		catch (error) {
			return done(error)
		}
	})

	it("delete non-existent file, non-recursive", function (done) {
		let s = getSink()
		try {

			let promise = s.rm('testfile4', {
				recursive: false
			})
			promise.then((data) => {
				// Normally this would be an error, but S3 seems not to care
				// It would take an extra query for us to tell if there was
				// really a file to delete, so let's not make this slow for
				// no reason.
				done()
			})
				.catch(err => {
					done()
				})
		}
		catch (error) {
			return done(error)
		}
	})


	it("a directory create", function (done) {
		let s = getSink()
		try {

			let promise = s.mkdir('testdir')
			promise.then(async (data) => {
				let info = await s.getFullFileInfo('testdir')
				assert.equal('testdir', info.name)
				done()
			})
		}
		catch (error) {
			return done(error)
		}
	})
	
	it("a recursive directory create", function (done) {
		let s = getSink()
		let dirName = 'testdir2/testdir3'

		async function run() {
			try {
				await s.rm('testdir2', {
					recursive: true
				})
			}
			catch (e) { }

			try {
				// okay, so we don't set this to be recursive, so it should probably fail,
				// but that would require extra queries to make it fail, so we'll just let it
				// succeed.
				await s.mkdir(dirName)
			}
			catch (e) {
				done(new Error('directory create failed'))
			}

			try {
				await s.mkdir(dirName, {
					recursive: true
				})
				let info = await s.getFullFileInfo(dirName)
				assert.equal('testdir3', info.name)
				await s.rm('testdir2', {
					recursive: true
				})
				done()
			}
			catch (e) {
				done(new Error(e))
			}
		}
		run()
	})

	it("find all paths", function (done) {
		let s = getSink()
		let f = async () => {
			try {
				await s.write('testdir/data1.txt', msg)
				await s.write('testdir/data2.txt', msg)
				await s.write('testdir/data3.txt', msg)
				let promise = s.findPaths()
				promise.then((data) => {
					if (data.length == 6) {
						done()
					}
					else {
						done(new Error('the directory did not contain the right number of files'))
					}
				})
				.catch(err => {
					done(err)
				})
			}
			catch (error) {
				return done(error)
			}
		}
		f()
	})
	it("a directory read", function (done) {
		let s = getSink()
		try {
			let promise = s.getFullFileInfo('testdir')
			promise.then((data) => {
				if(data.name.endsWith('/')) {
					return done(new Error('Name should not end with slash'))
				}
				if(data.relPath.endsWith('/')) {
					console.log(data)
					return done(new Error('relPath should not end with slash'))
				}
				if (data.children.length == 3) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			})
		}
		catch (error) {
			return done(error)
		}
	})
	it("a directory read with ending slash", function (done) {
		let s = getSink()
		try {
			let promise = s.getFullFileInfo('testdir/')
			promise.then((data) => {
				if(data.name.endsWith('/')) {
					return done(new Error('Name should not end with slash'))
				}
				if(data.relPath.endsWith('/')) {
					return done(new Error('relPath should not end with slash'))
				}
				if (data.children.length == 3) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			})
		}
		catch (error) {
			return done(error)
		}
	})
	it("find all data paths", function (done) {
		let s = getSink()
		try {
			let promise = s.findPaths({
				namePattern: 'data'
			})
			promise.then((data) => {
				if (data.length == 5) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			})
		}
		catch (error) {
			return done(error)
		}
	})
	it("find all data paths with real regexp", function (done) {
		let s = getSink()
		try {
			let promise = s.findPaths({
				namePattern: /data/
			})
			promise.then((data) => {
				if (data.length == 5) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			})
		}
		catch (error) {
			return done(error)
		}
	})
	it("find all data paths with real regexp (path)", function (done) {
		let s = getSink()
		try {
			let promise = s.findPaths({
				pathPattern: /data/
			})
			promise.then((data) => {
				if (data.length == 5) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			})
		}
		catch (error) {
			return done(error)
		}
	})
	it("find all data paths with function", function (done) {
		let s = getSink()
		try {
			let promise = s.findPaths({
				namePattern: function(name) {
					return name.indexOf('data') > -1
				}
			})
			promise.then((data) => {
				if (data.length == 5) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			})
		}
		catch (error) {
			return done(error)
		}
	})
	it("find all data paths with function", function (done) {
		let s = getSink()
		try {
			let promise = s.findPaths({
				namePattern: function(name) {
					return name.indexOf('data') > -1
				}
				, pathPattern: async function(name) {
					return name.indexOf('2') > -1
				}
			})
			promise.then((data) => {
				if (data.length == 2) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			})
		}
		catch (error) {
			return done(error)
		}
	})
	it("find just directory paths", function (done) {
		let s = getSink()
		try {
			let promise = s.findPaths({
				file: false
			})
			promise.then((data) => {
				if (data.length == 1) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			})
		}
		catch (error) {
			return done(error)
		}
	})
	it("a directory delete", function (done) {
		let s = getSink()
		try {
			let promise = s.rm('testdir')
			promise.then(async (data) => {
				let paths = await s.findPaths()
				if(paths.length == 2) {
					done()
				}
				else {
					done(new Error('Wrong number of files after delete.'))
				}
			})
			.catch(err => {
				done(err)
			})
		}
		catch (error) {
			return done(error)
		}
	})
	it("a sync read", function (done) {
		let s = getSink()
		try {
			let data = s.readSync('data1.txt')
			done(new Error('should have thrown error'))
		}
		catch (error) {
			// This is fine since the s3 sink should throw an error for this method
			return done()
		}
	})

	it("a directory read with no directory entry", function (done) {
		let s = getSink()
		let f = async () => {
			try {
				await s.write('testdir2/data1.txt', msg)
				await s.write('testdir2/data2.txt', msg)
				await s.write('testdir2/data3.txt', msg)
				let data = await s.getFullFileInfo('testdir2')
				if(data.name.endsWith('/')) {
					return done(new Error('Name should not end with slash'))
				}
				if(data.relPath.endsWith('/')) {
					return done(new Error('relPath should not end with slash'))
				}
				if (data.children.length == 3) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			}
			catch (error) {
				return done(error)
			}
			finally {
				await s.rm('testdir2')
			}
		}
		f()
	})

	it("a directory read with no directory entry and ending slash", function (done) {
		let s = getSink()
		let f = async () => {
			try {
				await s.write('testdir2/data1.txt', msg)
				await s.write('testdir2/data2.txt', msg)
				await s.write('testdir2/data3.txt', msg)
				let data = await s.getFullFileInfo('testdir2/')
				if(data.name.endsWith('/')) {
					return done(new Error('Name should not end with slash'))
				}
				if(data.relPath.endsWith('/')) {
					return done(new Error('relPath should not end with slash'))
				}
				if (data.children.length == 3) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			}
			catch (error) {
				return done(error)
			}
			finally {
				await s.rm('testdir2')
			}
		}
		f()
	})

	it("two levels of directory read with no directory entries", function (done) {
		let s = getSink()
		let f = async () => {
			try {
				await s.write('testdir2/testdir3/data1.txt', msg)
				await s.write('testdir2/testdir3/data2.txt', msg)
				await s.write('testdir2/testdir3/data3.txt', msg)
				let data = await s.getFullFileInfo('testdir2/')
				let child = data.children[0]
				if(!child) {
					return done(new Error("Should be one child"))
				}
				if(child.name != 'testdir3') {
					return done(new Error('child should be testdir3'))
				}
				if(child.name.endsWith('/')) {
					return done(new Error('Name should not end with slash'))
				}
				if(child.relPath.endsWith('/')) {
					return done(new Error('relPath should not end with slash'))
				}
				if (data.children.length == 1) {
					done()
				}
				else {
					done(new Error('the directory did not contain the right number of files'))
				}
			}
			catch (error) {
				return done(error)
			}
			finally {
				await s.rm('testdir2')
			}
		}
		f()
	})

})