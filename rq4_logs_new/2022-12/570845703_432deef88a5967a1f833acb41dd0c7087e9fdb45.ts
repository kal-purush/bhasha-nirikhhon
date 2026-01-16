import jetpack from 'fs-jetpack';
import chalk from 'chalk';
export const getPathsFromPackageLock = (
  oldLockPath: string,
  newLogPath: string
) => {
  const oldLock = jetpack.read(oldLockPath, 'json');
  const newLock = jetpack.read(newLogPath, 'json');
  const oldPackages = Object.keys(oldLock.packages).filter(
    (path: string) => path
  );
  const newPackages = Object.keys(newLock.packages).filter(
    (path: string) => path
  );
  const addedPackages = newPackages.filter(
    (path: string) => !oldPackages.includes(path)
  );
  const removedPackages = oldPackages.filter(
    (path: string) => !newPackages.includes(path)
  );
  const changedPackages = newPackages.filter(
    (path: string) =>
      oldPackages.includes(path) &&
      oldLock.packages[path].version !== newLock.packages[path].version
  );
  const changedFiles = [
    ...addedPackages.map((path: string) => `${path}/**/*`),
    // ...removedPackages,
    ...changedPackages.map((path: string) => `${path}/**/*`),
  ];
  return changedFiles;
};