/**
 * This test is to make sure that any packages consumed in the dist or dist-cjs folders are defined
 * in either the dependencies or peer-dependencies portions of the package.json
 *
 * If there is a package mismatch either:
 *  - There can be a runtime failure due to a missing dependency
 *  - There is package bloat causing longer npm install times
 */

import { describe, expect, it } from "@jest/globals";
import fs from "node:fs";
import path from "node:path";
import packageJson from "../../package.json";

const processRelativeDistEsmFolder = "./dist";
const processRelativeDistCjsFolder = "./dist-cjs";

function getPackagesThatWillInstallForConsumers() {
  const defaultEmptyPackageJsonDependencies = {
    dependencies: {},
    peerDependencies: {},
  };
  const { dependencies, peerDependencies } = {
    ...defaultEmptyPackageJsonDependencies,
    ...packageJson,
  };
  const consumerInstalledDependencies = new Set([...Object.keys(dependencies), ...Object.keys(peerDependencies)]);
  return consumerInstalledDependencies;
}

function getFilePathsRecursively(directory: string, extensionsToMatch: Array<string> = []) {
  const result: Array<string> = [];
  const subpathsInDirectory = fs.readdirSync(directory);

  for (const subpath of subpathsInDirectory) {
    const filepath = path.join(directory, subpath);
    const isDirectory = fs.statSync(filepath).isDirectory();

    if (isDirectory) {
      const filesInDirectory = getFilePathsRecursively(filepath, extensionsToMatch);
      result.push(...filesInDirectory);
    } else {
      const fileExtension = path.extname(filepath);
      const isFileAMatch = extensionsToMatch.includes(fileExtension);
      if (isFileAMatch) {
        result.push(filepath);
      }
    }
  }

  return result;
}

function readImportPathsFromFile(path: string) {
  const importRegex = /(im|ex)port.* "(.*)"/;

  const lines = fs
    .readFileSync(path, {
      encoding: "utf-8",
    })
    .split("\n");
  const importRegexMatch = lines.map((line) => {
    const importMatch = line.split(importRegex);
    return importMatch[2] ?? null;
  });
  const importPathsOnly = importRegexMatch.filter((match): match is string => {
    return match != null;
  });
  return importPathsOnly;
}

function readRequirePathsFromFile(path: string) {
  const requireRegex = /require\("(.*)"\)/;

  const lines = fs
    .readFileSync(path, {
      encoding: "utf-8",
    })
    .split("\n");
  const requireRegexMatch = lines.map((line) => {
    const requireMatch = line.split(requireRegex);
    return requireMatch[1] || null;
  });
  const requirePathsOnly = requireRegexMatch.filter((match): match is string => {
    return match != null;
  });
  return requirePathsOnly;
}

describe("Checking packages against dist folders", () => {
  const allDistEsmJsFiles = getFilePathsRecursively(processRelativeDistEsmFolder, [".js"]);
  const allDistCjsJsFiles = getFilePathsRecursively(processRelativeDistCjsFolder, [".js"]);
  const allImportPathsInEsmDist = allDistEsmJsFiles.flatMap(readImportPathsFromFile);
  const allImportPathsInCjsDist = allDistCjsJsFiles.flatMap(readRequirePathsFromFile);
  const allImportPathsInDist = [...allImportPathsInCjsDist, ...allImportPathsInEsmDist];
  const dependentPackageNamesInDist = allImportPathsInDist
    .filter((importPath) => {
      // if the path starts with a . it's relative
      const isRelativePath = importPath[0] === ".";
      const isNodePackage = importPath.startsWith("node");
      return !isRelativePath && ~isNodePackage;
    })
    .map((importPath) => {
      const pathParts = importPath.split("/");
      if (pathParts[0][0] === "@") {
        // If the first item starts with a "@" it's a scoped package
        // E.g. `@jest/globals`
        return `${pathParts[0]}/${pathParts[1]}`;
      } else {
        // Otherwise the first item is the full package
        // E.g. mime/lite
        return pathParts[0];
      }
    });

  const dependentPackageNamesFromDist = new Set(dependentPackageNamesInDist);
  const dependentPackageNamesFromJson = getPackagesThatWillInstallForConsumers();

  it("declares all packages consumed in the dist folders as part of the package.json's dependencies sections", () => {
    const distPackageNames = new Set(dependentPackageNamesFromDist);
    dependentPackageNamesFromJson.forEach((jsonPackageName) => {
      distPackageNames.delete(jsonPackageName);
    });

    expect(() => {
      if (distPackageNames.size !== 0) {
        throw `The dist folders contains package(s) that are not found in the package.json dependencies. Move the following to your dependencies: ${new Intl.ListFormat().format(distPackageNames)}`;
      }
    }).not.toThrow();
  });

  it("all packages declared in the package.json dependencies sections are consumed by the dist folders", () => {
    const jsonPackageNames = new Set(dependentPackageNamesFromDist);
    dependentPackageNamesFromDist.forEach((distPackageName) => {
      jsonPackageNames.delete(distPackageName);
    });

    expect(() => {
      if (jsonPackageNames.size !== 0) {
        throw `The package.json contains package(s) that are not found in the dist output. Move the following to your dev-dependencies: ${new Intl.ListFormat().format(jsonPackageNames)}`;
      }
    }).not.toThrow();
  });
});