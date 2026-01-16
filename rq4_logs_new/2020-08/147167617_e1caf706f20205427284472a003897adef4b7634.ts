#!/usr/bin/env node

import validate, { IsValidDirectory } from './index.js'
const path = process.argv[2] || process.cwd()

validate(path)
	.then(([isValid, invalidPaths]: IsValidDirectory) => {
		if (isValid) {
			console.log(`${path} is valid`)
		} else {
			console.error(
				`${path} is invalid, due to the following paths:\n`,
				invalidPaths
			)
		}
	})
	.catch((err) => {
		console.error(`can't validate ${path}, due to:\n${err.message}`)
		if (!process.exitCode) {
			process.exitCode = 2
		}
	})