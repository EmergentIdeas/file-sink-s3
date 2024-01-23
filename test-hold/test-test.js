import mocha from "mocha"
import { assert } from 'chai'

import {
	S3Client, ListBucketsCommand, PutObjectCommand,
	DeleteObjectCommand, GetObjectCommand, ListObjectsV2Command, GetObjectAttributesCommand
} from "@aws-sdk/client-s3"
import credentials from '../credentials/test-credentials.mjs'

let bucket = 'file-sink-s3-test-1'
const client = new S3Client(credentials)
let fileKey = 'f' + (new Date()).getTime()

describe("a basic test of the aws toolkit", function () {

	// it("list buckets", async function () {
	// 	let command = new ListBucketsCommand({})

	// 	try {
	// 		let data = await client.send(command)
	// 		console.log(data)
	// 	}
	// 	catch (e) {
	// 		console.error(e)
	// 	}
	// })

	// it("put file", async function () {
	// 	let command = new PutObjectCommand({
	// 		Bucket: bucket
	// 		, Key: 'test1/test3/' + fileKey
	// 		, Body: 'abcd'
	// 	})

	// 	try {
	// 		let data = await client.send(command)
	// 		console.log(data)
	// 	}
	// 	catch (e) {
	// 		console.error(e)
	// 	}
	// })

	// it("delete file", async function () {
	// 	let command = new DeleteObjectCommand({
	// 		Bucket: bucket
	// 		, Key: fileKey
	// 	})

	// 	try {
	// 		let data = await client.send(command)
	// 		console.log(data)
	// 	}
	// 	catch (e) {
	// 		console.error(e)
	// 	}
	// })

	// it("get file contents", async function () {
	// 	let command = new GetObjectCommand({
	// 		Bucket: bucket
	// 		, Key: 'f1705088983567'
	// 	})

	// 	try {
	// 		let data = await client.send(command)
	// 		/*
	// 		    Body object has 'transformToByteArray', 'transformToWebStream', and 'transformToString'
	// 		*/
	// 		let buffer = await data.Body.transformToByteArray()
	// 		console.log(buffer)
	// 		let enc = new TextDecoder("utf-8")
	// 		console.log(enc.decode(buffer))
	// 	}
	// 	catch (e) {
	// 		console.error(e)
	// 	}
	// })

	it("calculate hash", async function () {
		let command = new GetObjectAttributesCommand({
			Bucket: bucket
			, Key: 'f1705688330630data1.txt'
			, ObjectAttributes: [ "Checksum", "ETag", "ObjectParts", "StorageClass", "ObjectSize"]
		})

		try {
			let data = await client.send(command)
			console.log(JSON.stringify(data, null, '\t'))
		}
		catch (e) {
			console.error(e)
		}
	})

	// it("list contents", async function () {
	// 	let maxKeys = 2
	// 	let prefix = 'test1/'
	// 	let command = new ListObjectsV2Command({
	// 		Bucket: bucket
	// 		, Prefix: prefix
	// 		, Delimiter: '/'
	// 		, MaxKeys: maxKeys
	// 	})

	// 	try {
	// 		let data = await client.send(command)
	// 		console.log(data)
	// 		while (data.NextContinuationToken && data.IsTruncated) {
	// 			command = new ListObjectsV2Command({
	// 				Bucket: bucket
	// 				, Prefix: prefix
	// 				, MaxKeys: maxKeys
	// 				, Delimiter: '/'
	// 				, ContinuationToken: data.NextContinuationToken
	// 			})

	// 			data = await client.send(command)
	// 			console.log(data)
	// 		}
	// 	}
	// 	catch (e) {
	// 		console.error(e)
	// 	}
	// })

})