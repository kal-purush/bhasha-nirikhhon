const { Command } = require('./types')

const cloneCommand: Command = {
  name: "clone",
  methodToCall: (args) => {
    const appName = args['_'][1]
    const method = args['p'] || "ssh"
    if (repoList[appName]) {
      const repoUrl = repoList[appName][method]
      if (!repoUrl) {
        return console.log(chalk.red(`${method} schema not found for ${appName}`))
      }
      console.log(chalk.green(`cloning ${appName} from ${repoUrl}`))
      simpleGit.outputHandler((_, stdout, stderr) => {
        stdout.pipe(process.stdout);
        stderr.pipe(process.stderr);
      })
      .clone(repoList[appName][method])
      .then(() => console.log(chalk.green(`successfully cloned ${appName} from ${repoUrl}`)))
      .catch((error) => console.log(chalk.red(error)))
    } else {
      console.log(`project ${chalk.red(appName)} doesn't found`)
    }
  }
}

module.exports = cloneCommand