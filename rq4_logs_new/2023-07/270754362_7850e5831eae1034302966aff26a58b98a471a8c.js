// eslint-disable-next-line @typescript-eslint/no-unused-vars
const UninstallAPI = require('@quasar/app-vite/lib/app-extension/UninstallAPI');

const fs = require('fs');
const path = require('path');
const getCallerPath = require('@quasar/app-vite/lib/helpers/get-caller-path');

/**
 * @param {UninstallAPI} api
 * @param {string} templatePath
 * @param {string[]} knownPaths
 * @param {string[]} excludePaths
 */
module.exports = function (api, templatePath, knownPaths, excludePaths) {
  const dir = getCallerPath();
  const absolutePath = path.resolve(dir, templatePath);

  const paths = fs.readdirSync(absolutePath);

  for (const currentPath of paths) {
    removePath(api, absolutePath, currentPath, [
      ...(knownPaths || []),
      ...(excludePaths || []),
    ]);
  }

  knownPaths?.forEach((value) => api.removePath(value));
};

/**
 * @param {UninstallAPI} api
 * @param {string} templatePath
 * @param {string} relativePath
 * @param {string[]} knownPaths
 * @param {string[]} excludePaths
 */
function removePath(api, templatePath, relativePath, excludePaths) {
  const absolutePath = path.resolve(templatePath, relativePath);

  if (excludePaths?.includes(relativePath)) {
    return;
  }

  if (fs.lstatSync(absolutePath).isFile()) {
    api.removePath(relativePath);
  } else {
    const paths = fs.readdirSync(absolutePath);

    for (const currentPath of paths) {
      removePath(
        api,
        templatePath,
        path.join(relativePath, currentPath),
        excludePaths
      );
    }
  }
}