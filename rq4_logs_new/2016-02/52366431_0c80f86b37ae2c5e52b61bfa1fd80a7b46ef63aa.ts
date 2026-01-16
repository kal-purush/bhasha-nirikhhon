/// <reference path="../typings/main.d.ts" />

import inquirer = require('inquirer');
import semver = require('semver');

const path = require('path');
const fs = require('fs');

export default class PackageConfig {

    constructor(
        private _packageConfigDir: string
    ) {}

    private _packageConfigPath = path.join(this._packageConfigDir,'package.json');
    private _packageConfig = require(this._packageConfigPath);

    get fullName(): string {
        return this._packageConfig.name;
    }

    get version(): string {
        return this._packageConfig.version
    }
    set version(version: string) {
        this._packageConfig.version = version;
        this.save();
    }

    updateVersion(release: string) {
        this.version = semver.inc(this._packageConfig.version, release);
    }

    dependencyIntersection(dependencies: Array<string>): any {
        const intersectingDependencies = {};
        Object.keys(this._packageConfig.dependencies)
            .forEach((fullDependency) => {
                const indexOfScope = fullDependency.indexOf('/');
                let dependency = fullDependency;
                if (indexOfScope > -1) {
                    dependency = dependency.substr(indexOfScope + 1, dependency.length);
                }
                if (dependencies.indexOf(dependency) > -1) {
                    intersectingDependencies[dependency] = fullDependency;
                }
            });
        return intersectingDependencies;
    }

    isPackageUsed(packageName: string): boolean {
        return !!this._packageConfig.dependencies[packageName]
    }

    isPackageVersionSatisfied(packageName: string, version: string): boolean {
        const versionRange = this._packageConfig.dependencies[packageName];
        return semver.satisfies(version, versionRange);
    }

    getPackageVersion(packageName: string): string {
        return this._packageConfig.dependencies[packageName];
    }

    setPackageVersion(packageName: string, version: string) {
        this._packageConfig.dependencies[packageName] = version;
        this.save();
    }

    resolvePackageVersion(packageName: string, version: string): Promise<string> {
        if (this.isPackageUsed(packageName) &&
            !this.isPackageVersionSatisfied(packageName, version)) {
            return new Promise((resolve, reject) => {
                inquirer.prompt([
                    {
                        name: 'version',
                        message: `${this._packageConfig.name}:\nNew version ${version} for ${packageName} does not satisfy ${this.getPackageVersion(packageName)}.\nWhat do you want to update it to?`,
                        default: version
                    }
                ], (answers) => {
                    this.setPackageVersion(packageName, answers.version);
                    resolve(answers.version);
                });
            })
        } else {
            return Promise.resolve();
        }
    }

    save() {
        fs.writeFileSync(this._packageConfigPath, `${ JSON.stringify(this._packageConfig, null, 2) }\n`);
    }
}