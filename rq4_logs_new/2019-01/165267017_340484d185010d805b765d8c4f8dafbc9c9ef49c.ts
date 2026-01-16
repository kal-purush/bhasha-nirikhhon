import { Command, Helper, OptionsHelper } from '@dojo/cli/interfaces';
import * as path from 'path';
import * as chokidar from 'chokidar';
import { emptyDirSync } from 'fs-extra';
import chalk from 'chalk';

const foreground = require('foreground-child');
const NYC = require('nyc/index');
const sw = require('spawn-wrap');
const wrapper = require.resolve('nyc/bin/wrap.js');
const config = require('./nyc.json');

export interface CommandArgs {
	watch: true;
}

let isRunning = false;
let shouldRun = false;
const internPath = path.resolve('node_modules/.bin/intern');
const configPath = path.relative(process.cwd(), path.join(__dirname, 'intern.json'));

function runner() {
	isRunning = true;
	console.log(chalk.greenBright('Running unit tests...\n'));
	emptyDirSync(path.join(process.cwd(), 'output', 'coverage', '.cache'));
	const nyc = new NYC();
	nyc.createTempDirectory();
	nyc.addAllFiles();

	const env = {
		NYC_CONFIG: JSON.stringify(config),
		NYC_CWD: process.cwd(),
		NYC_ROOT_ID: nyc.rootId,
		NYC_INSTRUMENTER: config.instrumenter
	};

	sw([wrapper], env);

	process.exitCode = 0;
	foreground([internPath, `config=${configPath}`], () => {
		report(config);
		isRunning = false;
		if (shouldRun) {
			shouldRun = false;
			runner();
		}
	});
	function report(config: any) {
		process.env.NYC_CWD = process.cwd();
		const nyc = new NYC(config);
		nyc.report();
	}
}

const command: Command<CommandArgs> = {
	description: 'Run unit tests',
	register(options: OptionsHelper) {
		options('w', {
			alias: 'watch',
			describe: 'Re-runs unit tests when a file is changed',
			default: false
		});
	},
	run(helper: Helper, args: CommandArgs) {
		return new Promise(() => {
			if (args.watch) {
				const watcher = chokidar
					.watch([path.join(process.cwd(), 'src'), path.join(process.cwd(), 'tests')], {
						ignored: /node_modules/,
						ignoreInitial: true
					})
					.on('all', () => {
						if (!isRunning) {
							runner();
						} else {
							shouldRun = true;
						}
					});
				process.on('SIGINT', () => watcher.close());
			}
			runner();
		});
	}
};
export default command;