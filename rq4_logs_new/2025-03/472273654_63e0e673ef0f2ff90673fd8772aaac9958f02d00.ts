import fg from 'fast-glob';
import fs from 'fs';
import path from 'path';

const STORYBOOK_FILE_PATTERN = '**/*.stories.@(ts|tsx)';

const erMappe = (elementPath: string) => {
  return fs.statSync(elementPath).isDirectory();
};
const harPackageJson = (folderPath: string) => {
  return fs.existsSync(path.join(folderPath, 'package.json'));
};
const getPackageJson = (folderPath: string) => {
  const packageJsonPath = path.join(folderPath, 'package.json');
  return JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));
};

const harStories = async (folderPath: string) => {
  return await fg(STORYBOOK_FILE_PATTERN, { cwd: folderPath, absolute: true }).then(files => {
    return files.length > 0;
  });
};

type Config = {
  directory: string;
  titlePrefix: string;
  files: string;
};

export const lagStoriesEntriesForPakker = async (basePath: string): Promise<Config[]> => {
  const directories = fs
    .readdirSync(basePath)
    .map(element => path.join(basePath, element))
    .filter(erMappe);

  return Promise.all(
    directories.map(async (directory: string) => {
      if (harPackageJson(directory) && (await harStories(directory))) {
        return [
          {
            directory: path.join('..', directory, 'src'),
            titlePrefix: getPackageJson(directory).name as string,
            files: STORYBOOK_FILE_PATTERN,
          },
        ];
      }
      return lagStoriesEntriesForPakker(directory);
    }),
  ).then(res => res.flat());
};