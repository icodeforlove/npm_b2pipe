#!/usr/bin/env node
const Promise = require('bluebird');
const fs = require('mz/fs');
const request = require('request-promise');
const sha1 = Promise.promisify(require('node-sha1'));
const atmpt = require('atmpt');
const c = require('template-colors');
const prettyBytes = require('pretty-bytes');
const prettyMs = require('pretty-ms');
const bitrate = require('bitrate');

const argv = require('yargs')
	.describe('concurrency', 'max concurrent connections')
	.describe('path', 'path on b2 container')
	.describe('bucket', 'bucket on b2')
	.describe('type', 'content-type for the file being uploaded')
	.describe('account', 'b2 account')
	.describe('key', 'b2 key')
	.describe('silent', 'will not output to stderr')
	.describe('attempts', 'max upload attempts')
	.default('attempts', 30)
	.default('type', 'raw')
	.default('silent', false)
	.default('concurrency', 20)
	.default('chunk', 5000000)
	.demandOption([
		'path',
		'bucket',
		'account',
		'key'
	])
	.argv;

let b2 = {};
let buffersQueue = [];
let bufferIndex = 1;
let scratchBuffer = [];
let stdinCompleted = false;
let completed = false;
let uploads = [];
let stdinPaused = false;
let concurrency = argv.concurrency;
let uploadedBytes = 0;
let uploadedParts = [];
let isLargeFile = false;
let isSmallFile = false;
const stdin = process.openStdin();

let b2authorized = b2authorize();
let b2uploadStarted = Promise.pending();

if (process.stdin.isTTY) {
	console.error('- missing stdin stream');
	process.exit();
}

function log (message) {
	if (!argv.silent) {
		console.log(String(message));
	}
}

log(c`b2 upload started`.green.bold);
log(c`- concurrency = ${concurrency}.white.bold`.grey);

stdin.on('data', async chunk => {
	scratchBuffer.push(chunk);

	let scratchBufferSize = scratchBuffer.map(buffer => buffer.length).reduce((a, b) => a + b, 0);

	if (scratchBufferSize > argv.chunk) {

		if (bufferIndex === 1) {
			isLargeFile = true;
			log(c`- uploadtype = ${'largefile'}.white.bold`.grey);
			uploadStart();
		}

		buffersQueue.push({
			index: bufferIndex,
			scratchBuffer
		});

		scratchBuffer = [];

		bufferIndex++;
	}

	if (buffersQueue.length >= concurrency) {
		stdinPaused = true;
		process.stdin.pause();
	}
});

stdin.on('end', async () => {
	stdinCompleted = true;
	if (scratchBuffer.length) {
		let scratchBufferSize = scratchBuffer.map(buffer => buffer.length).reduce((a, b) => a + b, 0);

		if (bufferIndex > 1 || scratchBufferSize > argv.chunk) {
			buffersQueue.push({
				index: bufferIndex,
				scratchBuffer
			});
		} else {
			isSmallFile = 1;
			log(c`- uploadtype = ${'standard'}.white.bold`.grey);
			uploadedBytes = scratchBufferSize;
			b2uploadStarted.resolve();
			await b2uploadFile(scratchBuffer);
			completed = 1;
		}
		scratchBuffer = [];
	}
});

function storePart(part) {
	return new Promise(async (resolve, reject) => {
		await uploadPart(part);
		resolve();
	});
}

(async () => {
	await b2uploadStarted.promise;

	while (!completed && (buffersQueue.length || uploads.length || !stdinCompleted)) {
		if (uploads.length < concurrency && buffersQueue.length) {
			let itemsToUpload = concurrency - uploads.length;

			for (let i = 0; i < itemsToUpload && buffersQueue.length; i++) {
				uploads.push(
					storePart(buffersQueue.shift())
				);
			}
		}

		uploads = uploads.filter(upload => !upload.isResolved());

		if (!uploads.length && !buffersQueue.length && stdinCompleted) {
			completed = true;
		} else {
			await new Promise(resolve => setImmediate(resolve));
		}

		if (!stdinCompleted && stdinPaused && buffersQueue.length + uploads.length < concurrency) {
			stdinPaused = false;
			process.stdin.resume();
		}
	}
})().catch(console.error);

async function b2authorize () {
	let {
		apiUrl,
		authorizationToken
	} = await request.get({
		url: 'https://api.backblazeb2.com/b2api/v2/b2_authorize_account',
		auth: {
			user: argv.account,
			password: argv.key
		},
		json: true
	});

	log(c`- apiUrl = ${apiUrl}.white.bold\n`.grey);

	b2.apiUrl = apiUrl;
	b2.authorizationToken = authorizationToken;
}

async function b2startUpload ({path, type}) {
	return await atmpt(async attempt => {
		log(c`initiating upload`.green);

		const {
			fileId
		} = await request.post({
			url: `${b2.apiUrl}/b2api/v2/b2_start_large_file`,
			headers: {
				Authorization: b2.authorizationToken
			},
			json: {
				fileName: path,
				bucketId: argv.bucket,
				contentType: type
			}
		});

		b2.fileId = fileId;
	}, {maxAttempts: argv.attempts, delay: attempt => attempt * 1000});
}

async function b2uploadPart (part) {
	return await atmpt(async attempt => {
		let buffer = Buffer.concat(part.scratchBuffer);
		let bufferSha1 = await sha1(buffer);

		let uploadUrlInfo = await request.post({
			url: `${b2.apiUrl}/b2api/v2/b2_get_upload_part_url`,
			headers: {
				Authorization: b2.authorizationToken
			},
			json: {
				fileId: b2.fileId
			}
		});

		await request.post({
			url: uploadUrlInfo.uploadUrl,
			headers: {
				Authorization: uploadUrlInfo.authorizationToken,
				'X-Bz-Part-Number': part.index,
				'X-Bz-Content-Sha1': bufferSha1,
			},
			body: buffer
		});

		uploadedParts.push({
			index: part.index,
			sha1: bufferSha1,
			bytes: buffer.length
		});
	}, {maxAttempts: argv.attempts, delay: attempt => attempt * 1000});
}

async function b2uploadFile (scratchBuffer) {
	return await atmpt(async attempt => {
		await b2authorized;

		let buffer = Buffer.concat(scratchBuffer);
		let bufferSha1 = await sha1(buffer);

		let uploadUrlInfo = await request.post({
			url: `${b2.apiUrl}/b2api/v2/b2_get_upload_url`,
			headers: {
				Authorization: b2.authorizationToken
			},
			json: {
				bucketId: argv.bucket
			}
		});

		await request.post({
			url: uploadUrlInfo.uploadUrl,
			headers: {
				Authorization: uploadUrlInfo.authorizationToken,
				'X-Bz-Content-Sha1': bufferSha1,
				'X-Bz-File-Name': argv.path,
				'Content-Type': argv.type
			},
			body: buffer
		});
	}, {maxAttempts: argv.attempts, delay: attempt => attempt * 1000});
}

async function b2completeUpload () {
	return await atmpt(async attempt => {
		await request.post({
			url: `${b2.apiUrl}/b2api/v2/b2_finish_large_file`,
			headers: {
				Authorization: b2.authorizationToken
			},
			json: {
				fileId: b2.fileId,
				partSha1Array: uploadedParts.sort((a, b) => a.index - b.index).map(part => part.sha1)
			}
		});
	}, {maxAttempts: argv.attempts, delay: attempt => attempt * 1000});
}

async function uploadStart () {
	await b2authorized;
	await b2startUpload({
		path: argv.path,
		type: argv.type
	});
	b2uploadStarted.resolve();
}
async function uploadPart (part) {
	logProgress();
	await b2uploadPart(part);
	logProgress();
}
async function uploadComplete () {
	if (isLargeFile) {
		await b2completeUpload();
	}
}

function logProgress () {
	uploadedBytes = uploadedParts.map(part => part.bytes).reduce((a, b) => a + b, 0);
	if (process.stdout.clearLine && !argv.silent) {
		process.stderr.moveCursor(0, -1);
		process.stderr.clearLine();
		process.stdout.cursorTo(0);
		process.stdout.write(c`parts uploaded = ${String(uploadedParts.length)}.bold.white, active = ${String(uploads.length)}.bold.white, uploaded ${prettyBytes(uploadedBytes)}.bold.white\n`.grey.toString());
	}
}

(async () => {
	while (!isSmallFile && !isLargeFile) {
		await Promise.delay(10);
	}

	await b2uploadStarted.promise;

	let startTime = new Date();

	while (!completed) {
		await Promise.delay(50);
	}
	await uploadComplete();

	let duration = new Date() - startTime;

	log(c`\n\nupload completed in ${prettyMs(duration)}.bold.white, at ${bitrate(uploadedBytes, duration/1000, 'mbps').toFixed(2)}.bold.white mb/s`.green.bold);
})().catch(console.error);