import jetpack from 'fs-jetpack';
import minimatch from 'minimatch';
import chalk from 'chalk';

export const compareFiles = async (
  path: string,
  changedFiles: string[],
  commitNumber: number
): Promise<boolean> => {
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
    const rawFiles = (await coverageFolder.readAsync(file, 'json')) as string[];
    const coveredFiles = rawFiles.map((f) => f.replaceAll('\\', '/'));
    const changedFilesForTest = changedFiles
      .filter((f: string) =>
        coveredFiles.some((cf: string) => minimatch(cf, f))
      )
      .map((path) => {
        if (path.includes('node_modules')) {
          return chalk.yellow(
            `Dependency: ${path.split('node_modules/')[1].replace('/**/*', '')}`
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
      console.log(chalk.green(`\n${testName} has not covered changed files`));
    }
  }
  return nonFlakyFail;
};