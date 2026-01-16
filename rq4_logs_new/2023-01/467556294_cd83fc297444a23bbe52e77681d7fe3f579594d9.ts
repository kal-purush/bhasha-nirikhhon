/* eslint-disable */
// @ts-nocheck

// import fs from 'fs';
const fs = require('fs');

// function listFiles(targetDirectoryPath: string): string[] {
function listFiles(targetDirectoryPath) {
	return fs.readdirSync(targetDirectoryPath, { withFileTypes: true }).flatMap((dirent) => {
		const name = `${targetDirectoryPath}/${dirent.name}`;
		return dirent.isFile() ? [name] : listFiles(name);
	});
}

// const matchItemList: { [key: string]: string } = {};
const matchItemList = {};
listFiles('./src')
	.filter((filePath) => filePath.endsWith('.tsx') || filePath.endsWith('.ts'))
	.forEach((filePath) => {
		const contents = fs.readFileSync(filePath, 'utf-8');
		const lines = contents.split('\n');
		lines.forEach((line) => {
			const r = /i18n.t\('([^']*)'\)/g;
			let exec;
			while ((exec = r.exec(line)) != null) {
				if (matchItemList[exec[1]]) {
					console.log(`Duplicate key ${exec[1]}`);
				}
				matchItemList[exec[1]] = exec[1];
			}
		});
	});

fs.writeFileSync('./src/Common/Hooks/i18n-js/Lang.json', JSON.stringify(matchItemList, null, '\t'));