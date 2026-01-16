
"use strict";

// deps

	const path = require("path"),

		gulp = require("gulp"),
		eslint = require("gulp-eslint"),
		excludeGitignore = require("gulp-exclude-gitignore"),
		plumber = require("gulp-plumber");

// private

	var _gulpFile = path.join(__dirname, "gulpfile.js"),
		_libFiles = path.join(__dirname, "lib", "**", "*.js"),
		_unitTestsFiles = path.join(__dirname, "tests", "*.js"),
		_allJSFiles = [_gulpFile, _libFiles, _unitTestsFiles];

// tasks

	gulp.task("eslint", function () {

		return gulp.src(_allJSFiles)
			.pipe(plumber())
			.pipe(excludeGitignore())
			.pipe(eslint({
				"rules": {
					"indent": 0
				},
				"extends": "eslint:recommended"
			}))
			.pipe(eslint.format())
			.pipe(eslint.failAfterError());

	});

// watcher

	gulp.task("watch", function () {
		gulp.watch(_allJSFiles);
	});

// default

	gulp.task("default");