/// <reference path='../node_modules/@types/node/index.d.ts' />
/// <reference path='../node_modules/@types/benchmark/index.d.ts' />
import { Suite } from 'benchmark';
import memoryFs = require('memory-fs');

const mfs = new memoryFs();
const testPath = `/Dev/fusebox-chain-plugin/component/main.scss`;
const encodedTestPath = '/' + encodeURIComponent(testPath);

new Suite('memoryfs existsSync')
	.add('full', function() {
		mfs.existsSync(testPath);
	})
	.add('encoded', function() {
		// decodeURIComponent(encodedTestPath).slice(1);
		mfs.existsSync(encodedTestPath);
	})
	.on('start', ({currentTarget}) => {
		console.log(`${currentTarget.name} benchmark:`);
	})
	.on('cycle', function(event) {
		console.log(String(event.target));
	})
	.on('complete', ({currentTarget}) => {
		console.log(`${currentTarget.filter('fastest').map('name')} is fastest`);
	})
	// run async
	.run({ 'async': true });