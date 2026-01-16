const chalk = require('chalk');
const shell = require('shelljs');
const { join } = require('path');

const { readJson, saveJson } = require('./json');

const clone = (repositoryUrl, projectName) => {
  shell.exec(`git clone ${repositoryUrl} ${projectName}`);

  const packageJsonPath = join(projectName, 'package.json');
  const packageJsonData = readJson(packageJsonPath);
  const newPackageJsonData = {
    ...packageJsonData,
    name: projectName,
    description: '',
    repository: {},
    bugs: {},
    homepage: ''
  };

  console.log(chalk.green('Repository cloned!'));
  console.log(chalk.green('Editing package.json...'));

  saveJson(packageJsonPath, newPackageJsonData);

  console.log(chalk.green('Package.json edited!'));

  shell.cd(projectName);

  console.log(chalk.green('Installing dependencies...'));

  shell.exec('npm install');

  console.log(chalk.green('Dependencies installed!'));
  console.log(chalk.green('Editing README...'));
};

module.exports = {
  clone
};