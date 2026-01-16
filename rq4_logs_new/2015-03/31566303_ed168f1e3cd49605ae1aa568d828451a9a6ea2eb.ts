import fs = require('fs');
import fs_extra = require('fs-extra');
import mustache = require('mustache');
import react = require('react');
import react_router = require('react-router');

import fs_util = require('./fs_util');
import routes = require('./routes');
import scanner = require('./scanner');

function prerenderRoute(route: string, outputDir: string) {
	console.log(`pre-rendering ${route} to ${outputDir}`);
	var template = fs.readFileSync('index.html').toString();
	react_router.run(<react_router.Route>routes, route, (handler, state) => {
		var body = react.renderToString(react.createElement(handler, {params: state.params}));
		var html = mustache.render(template, {
			title: 'Page Title',
			body: body,
			appTheme: 'dist/theme.css',
			appScript: 'dist/app.js'
		});
		fs_extra.ensureDirSync(outputDir);
		fs.writeFileSync(`${outputDir}/index.html`, html);
	});
}

export function generateBlog(dir: string) {
	// remove and re-create output dir
	var config = scanner.readConfig(dir);
	fs_extra.ensureDirSync(config.outputDir);
	fs_util.cleanDir(config.outputDir);

	var prerenderedRoutes: string[] = [];

	var posts = scanner.readPosts(config);
	posts.forEach(post => {
		prerenderedRoutes.push(`/posts/${post.metadata.slug}`);
	});

	var tags = scanner.generateTagMap(posts);
	Object.keys(tags).forEach(tag => {
		prerenderedRoutes.push(`/posts/tagged/${tag}`);
	});

	prerenderedRoutes.forEach(route => {
		prerenderRoute(route, config.outputDir + route);
	});
}
