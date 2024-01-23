# File Sink

A slim abstraction over s3 to make it look like a file system. 

## Why

I want to use something else I wrote, [FileSink](https://www.npmjs.com/package/file-sink), to abstract
away just a little bit of the file system interface. I want callers to be able to read and write "files"
without needing to know where, specifically, on disk they're writing to, or even what they're really writing
to. It could be a file system, http, AWS S3, indexeddb, mongodb, or whatever. The caller shouldn't need to
know how to connect to the underlying resource, how to encrypt/decrypt, or anything more than the relative
path.

This provides node js access to s3 as a file system.

## Install

```bash
npm install file-sink-s3
```


## Selected Methods Summary

- read(path) - Reads file info, returns promise resolving to buffer
- readStream(path) - Reads file info as utf-8 text stream
- write(path, data) - Where data is a string, Buffer, TypedArray or DataView. Lots of options
for writing partial data/files as well. Returns promise.
- rm(path) - removes file or directory (recursive by default), returns promise
- mkdir(path) - makes directory, returns promise
- getFullFileInfo(path) - returns a promise with info about a file or directory. See format below.
- createHash(path) - A promise with the has value of the file data (sha512 by default)
- findPaths(options) - A bit like `find`, allows searching for files and directories by name
- find - Like findPaths, but instead of a promise it returns an EventEmitter which emits `data`
and `done` events. Each `data` event has a file info object. 

## Use

```
import FileSinkS3 from "file-sink-s3"

let sink = new FileSinkS3({
	bucket: 'file-sink-s3-test-1'
	, prefix: 'starting/point/'
	, connection: {
		region: 'us-east-1'
		, credentials: {
			accessKeyId: 'public_key'
			, secretAccessKey: 'secret_key'
		}
	}
})


// read my-file.txt
tempSink.read('my-file.txt', function(err, data) {
	console.log(data.toString())
})

let data = await tempSink.read('my-file.txt')

// read my-file.txt
let data = tempSink.readSync('my-file.txt')
// throws error if path does not exist or some other problem happens

// write to my-file.txt
tempSink.write('my-file.txt', 'Hello, World!', function(err) {
	// log error if exists
})

try {
	await tempSink.write('my-file.txt')
}
catch(err) {
	// log error
}


// read the info for a file like mod time
let info = await tempSink.getFullFileInfo('my-file.txt')
console.log(info.stat.mtime)

// find the names of the children of a directory

let dirInfo = await tempSink.getFullFileInfo('.')
for(let child of dirInfo.children) {
	console.log((child.directory ? '(d) ': "") + child.name)
}

```

## getFullFileInfo Data Format

Roughly, where `children` contains objects like this, but without the `children` attribute.

```
{
  name: 'testdir',
  parent: 'top-dir/test-data',
  stat: Stats {
    size: 4096,
    mtimeMs: 1701114609826.992,
    mtime: 2023-11-27T19:50:09.827Z
  },
  directory: true,
  relPath: 'testdir',
  accessUrl: 'https://file-sink-s3-test-1.s3.amazonaws.com/top-dir/test-data/testdir'
  children: []
}
```
