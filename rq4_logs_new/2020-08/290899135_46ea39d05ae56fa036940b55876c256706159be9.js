#!/usr/bin/env node

const { join } = require('path')
const { writeFileSync, existsSync, unlinkSync } = require('fs')
const sort = require('sort-package-json')

const xo = require('./package.json')

try {
	const root = process.cwd()
	let modified = false
	console.log('Working on root :', root)
	let target = join(root, 'package.json')

	const pkg = require(target)[('prettier', 'husky', 'xo')].forEach((tool) => {
		if (typeof pkg[tool] === 'undefined') {
			pkg[tool] = xo[tool]
			modified = true
		}
	})

	if (modified) {
		writeFileSync(target, JSON.stringify(sort(pkg), null, '\t'), 'utf8')
	}

	for (const file of ['.prettierrc', 'prettier.config.js']) {
		target = join(root, file)
		if (existsSync(target)) {
			unlinkSync(target)
		}
	}
} catch (error) {
	console.error('xo-quick postinstall failed :', error)
}