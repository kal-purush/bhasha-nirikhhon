import * as path from 'path';
const spawn = require('child_process').spawn;

export function createRunner(config, testFile) {
  // 1. node electron setup
  let options: any = {
    cwd: config.dir
  };
  if (options.env == null) {
    options.env = Object.create(process.env);
  }
  options.env.ATOM_SHELL_INTERNAL_RUN_AS_NODE = 1;
  options.env.DIR = config.dir;
  options.env.TUTORIAL_DIR = config.tutorialDir;
  options.env.TASK_POSITION = config.taskPosition;

  // 2. get absolute path to node exec
  let node = null;
  if (process.platform === 'darwin' && process.resourcesPath) {
    node = path.resolve(process.resourcesPath, '..', 'Frameworks', 'Atom Helper.app', 'Contents', 'MacOS', 'Atom Helper');
  } else {
    node = process.execPath;
  }

  // let runnerOptions = []; // setRunnerOptions(config);
  let ava = path.join(__dirname, '..', '..', 'ava', 'cli');
  let tapJson = path.join('..', '..', 'tap-json', 'bin', 'tap-json');

  // 3. spawn child process calling ava test runner
  return spawn(node, [
    // into shared node_modules directory
    ava,
    '--fail-fast',
    '--tap | ' + tapJson,
    testFile
  ], options);

}