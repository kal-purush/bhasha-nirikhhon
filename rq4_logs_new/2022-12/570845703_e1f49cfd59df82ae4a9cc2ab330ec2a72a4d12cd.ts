import {fetch} from '@whatwg-node/fetch';
import {Command} from 'commander';
import {XMLParser} from 'fast-xml-parser';
import util from 'util';
import chalk from 'chalk';
import {updatePackageScripts} from '../file-updates/package-scripts.js';
import {addDockerOverrides} from '../file-updates/docker-overrides.js';
import {updateGradleDependencies} from '../file-updates/gradle-dependencies.js';
import {updateOldCypressConfig} from '../file-updates/cypress-config.js';

const exec = util.promisify(require('child_process').exec);

const xmlParser = new XMLParser();

export function registerCreateBranchCommand(program: Command) {
  program
    .command('branch')
    .argument('<planKey>', 'Bamboo plan key to work on')
    .argument('<latestSuccess>', 'Latest successful run before analyzed run')
    .argument('<analyzedRun>', 'Analyzed run')
    .option('-t, --token <token>', 'Bamboo access token')
    .option(
      '-n, --new-cypress',
      'If the new cypress setup should be used',
    )
    .action(async (planKey, latestSuccess, analyzedRun, {token, newCypress}) => {
      console.log(chalk.green(`Setting up analysis for Build ${analyzedRun} of Plan ${planKey}`));
      const successfulBuildResponse = await fetch(
        `https://bamboobruegge.in.tum.de/rest/api/latest/result/${planKey}-${latestSuccess}`,
        {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      );
      const successfulBuildXml = await successfulBuildResponse.text();
      const successfulBuildData = xmlParser.parse(successfulBuildXml);
      const lastSuccessfulCommit = successfulBuildData.result.vcsRevisionKey;
      const analyzedBuildResponse = await fetch(
        `https://bamboobruegge.in.tum.de/rest/api/latest/result/${planKey}-${analyzedRun}`,
        {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        },
      );
      const analyzedBuildXml = await analyzedBuildResponse.text();
      const analyzedBuildData = xmlParser.parse(analyzedBuildXml);
      const analyzedCommit = analyzedBuildData.result.vcsRevisionKey;
      console.log(chalk.gray(`Creating branch for ${analyzedCommit}`));
      const {stdout: branchStdout, stderr: branchStderr} = await exec(
        `git checkout -b flaky-history/build-${analyzedRun} ${analyzedCommit}`,
      );
      if (branchStderr) {
        console.log(chalk.red(branchStderr));
      }
      console.log(chalk.gray(`Installing flaky test detection dependencies`));
      const {
        stdout: installStdout,
        stderr: installStderr,
      } = await exec(`cd src/test/cypress && npm i -D @heddendorp/coverage-git-compare @heddendorp/cypress-plugin-multilanguage-coverage`);
      if (installStderr) {
        console.log(chalk.red(installStderr));
      }
      console.log(chalk.gray(`Adding flaky test detection script to package.json`));
      updatePackageScripts(lastSuccessfulCommit);
      console.log(chalk.green(`Done!`));
      console.log(chalk.gray(`Adding Docker override files`));
      addDockerOverrides();
      console.log(chalk.green(`Done!`));
      console.log(chalk.gray(`Adding gradle dependencies`));
      updateGradleDependencies();
      console.log(chalk.green(`Done!`));
      console.log(chalk.gray(`Adding cypress plugin`));
      updateOldCypressConfig();
      console.log(chalk.green(`Done!`));
      console.log(chalk.gray(`Committing changes`));
      const {stdout: commitStdout, stderr: commitStderr} = await exec(
        `git add . && git commit -m "Add flaky test detection"`,
      );
      if (commitStderr) {
        console.log(chalk.red(commitStderr));
      }
      console.log(chalk.green(`Done!`));
      console.log(chalk.gray(`Pushing changes`));
      const {stdout: pushStdout, stderr: pushStderr} = await exec(
        `git push --set-upstream origin flaky-history/build-${analyzedRun}`,
      );
      if (pushStderr) {
        console.log(chalk.red(pushStderr));
      }
      console.log(chalk.green(`Done!`));
      console.log(chalk.green(`Branch created!`));
      console.log(chalk.gray(`Switching back to main branch`));
      const {stdout: checkoutStdout, stderr: checkoutStderr} = await exec(
        `git checkout develop`,
      );
      if (checkoutStderr) {
        console.log(chalk.red(checkoutStderr));
      }
      console.log(chalk.green(`Done!`));
    });
}