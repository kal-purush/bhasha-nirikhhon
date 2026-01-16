import { fetch } from '@whatwg-node/fetch';
import { Command } from 'commander';
import { XMLParser } from 'fast-xml-parser';
import util from 'util';
import chalk from 'chalk';
import { getPathsFromPackageLock } from '../helpers/package-lock-handler';
import jetpack from 'fs-jetpack';
import minimatch from 'minimatch';

const exec = util.promisify(require('child_process').exec);

const xmlParser = new XMLParser();
export function registerBambooCompareCommand(program: Command) {
  program
    .command('bamboo')
    .argument('<planKey>', 'Bamboo plan key')
    .argument('<bambooToken>', 'Bamboo access token')
    .option('-p, --path <path>', 'Path to coverage report', './coverage')
    .action(async (planKey, token, { path }) => {
      let lastSuccessfulCommit = 'HEAD^';
      const planResponse = await fetch(
        `https://bamboobruegge.in.tum.de/rest/api/latest/result/${planKey}`,
        {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      );
      const planXml = await planResponse.text();
      const planData = xmlParser.parse(planXml);
      const lastSuccessfulBuild = planData.results.results.result.find(
        (result: any) => result.buildState === 'Successful'
      )?.buildResultKey;
      if (lastSuccessfulBuild) {
        console.debug(`Last successful build: ${lastSuccessfulBuild}`);
        const successfulBuildResponse = await fetch(
          `https://bamboobruegge.in.tum.de/rest/api/latest/result/${lastSuccessfulBuild}`,
          {
            headers: {
              Authorization: `Bearer ${token}`,
            },
          }
        );
        const successfulBuildXml = await successfulBuildResponse.text();
        const successfulBuildData = xmlParser.parse(successfulBuildXml);
        lastSuccessfulCommit = successfulBuildData.result.vcsRevisionKey;
      } else {
        console.debug(
          'No successful build found, falling back to source branch'
        );
        const { stdout } = await exec(`git show-branch`);
        lastSuccessfulCommit = stdout.split('[')[1].split(']')[0];
      }
      const { stdout } = await exec(
        `git log ${lastSuccessfulCommit}...HEAD --pretty="@begin@%h@end@" --name-only`
      );
      const commitNumber = stdout.split('@begin@').length - 1;
      const files = stdout
        .split('\n')
        .map((line: string) => line.trim())
        .filter((line: string) => !line.includes('@begin@') && line.length);
      const changedFiles = Array.from(new Set<string>(files));
      if (changedFiles.some((file: string) => file === 'package-lock.json')) {
        console.log(
          chalk.gray(
            'package-lock.json changes detected, adding changed dependencies to list of changed files'
          )
        );
        const { stderr } = await exec(
          `git show ${lastSuccessfulCommit}:package-lock.json > old-package-lock.json`
        );
        if (stderr) {
          console.log(chalk.red(stderr));
          return;
        }
        const additionalFiles = getPathsFromPackageLock(
          'old-package-lock.json',
          'package-lock.json'
        );
        jetpack.remove('old-package-lock.json');
        changedFiles.push(...additionalFiles);
      }
      const coverageFolder = jetpack.cwd(path);
      const coverageFiles = await coverageFolder.findAsync({
        matching: '*files.json',
      });
      console.log(
        `Inspecting ${commitNumber} commits and ${coverageFiles.length} failed tests`
      );
      let nonFlakyFail = false;
      for (const file of coverageFiles) {
        const testName = file.split('-')[0];
        const rawFiles = (await coverageFolder.readAsync(
          file,
          'json'
        )) as string[];
        const coveredFiles = rawFiles.map((f) => f.replaceAll('\\', '/'));
        const changedFilesForTest = changedFiles
          .filter((f: string) =>
            coveredFiles.some((cf: string) => minimatch(cf, f))
          )
          .map((path) => {
            if (path.includes('node_modules')) {
              return chalk.yellow(
                `Dependency: ${path
                  .split('node_modules/')[1]
                  .replace('/**/*', '')}`
              );
            }
            return path;
          });
        if (changedFilesForTest.length > 0) {
          nonFlakyFail = true;
          console.log(
            chalk.red(
              `\nTest ${testName} has covered ${chalk.bold(
                changedFilesForTest.length
              )} changes that appeared in your commit range`
            )
          );
          console.log(changedFilesForTest.join(`\n`));
        } else {
          console.log(
            chalk.green(`\n${testName} has not covered changed files`)
          );
        }
      }
      console.log();
      if (nonFlakyFail) {
        process.exit(1);
      }
    });
}