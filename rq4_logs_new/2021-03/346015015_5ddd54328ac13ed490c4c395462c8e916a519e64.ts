import { execSync } from 'child_process';
import { join } from 'path';
import * as open from 'open';
import { Command, Option } from 'commander';
import { getOrElse, tryCatch } from 'fp-ts/lib/Either';
import { flow, pipe } from 'fp-ts/lib/function';
import { exitProcessError, readJsonFile, throwError } from './utils';

const MASTER_BRANCH_NAME = 'main';
const SCRIPT_OPTION = {
  type: 'type',
} as const;

const PACKAGE_VERSION = {
  major: 'major',
  minor: 'minor',
  patch: 'patch',
} as const;
type PackageVersion = keyof typeof PACKAGE_VERSION;

const getOptions = () =>
  new Command()
    .addOption(
      new Option(
        `-t, --${SCRIPT_OPTION.type} [${Object.values(PACKAGE_VERSION).join(' | ')}]`,
        'update package version type',
      )
        .choices(Object.values(PACKAGE_VERSION))
        .default('minor'),
    )
    .parse(process.argv)
    .opts();

const readPackageVersion = () =>
  pipe(
    readJsonFile<{ version: string }>('package.json'),
    getOrElse<Error, { version: string }>(throwError),
    ({ version }) => version,
  );

const readGitRemoteUrl = () => execSync('git config --get remote.origin.url').toString().replace('.git', '');

const updatePackageVersion = (packageVersion: PackageVersion) =>
  pipe(execSync('git pull'), () => execSync(`npm version ${packageVersion}`));

const readCurrentBranchName = (): string =>
  pipe(execSync('git branch --contains'), (buf) => buf.toString().split('\n')[0].split(' ')[1]);

const pushReleaseBranch = flow(
  updatePackageVersion,
  readPackageVersion,
  (version) => {
    const branchName = `release/v${version}`;
    // eslint-disable-next-line no-console
    console.log(`new version is ${version}, checkout "${branchName}" branch`);
    return branchName;
  },
  (branchName) => {
    execSync(`git checkout -b ${branchName}`);
    execSync(`git push -u ${branchName}`);
    return branchName;
  },
  (branchName) => {
    const remoteUrl = readGitRemoteUrl();
    const pullRequestUrl = join(remoteUrl, 'pull', 'new', branchName);
    open(pullRequestUrl);
  },
);

const main = () =>
  tryCatch(() => {
    const options = getOptions();
    const packageVersion: PackageVersion = options[SCRIPT_OPTION.type];

    const branchName = readCurrentBranchName();
    if (branchName !== MASTER_BRANCH_NAME) {
      throw new Error(`current branch is ${branchName}, please switch to "${MASTER_BRANCH_NAME}" branch`);
    }

    pushReleaseBranch(packageVersion);
    process.exit(0);
  }, exitProcessError);

main();