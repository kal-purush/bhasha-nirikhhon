"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDependenciesChanges = exports.getManageDependenciesCommand = void 0;
const deep_object_diff_1 = require("deep-object-diff");
const fs_1 = __importDefault(require("fs"));
const child_process_1 = __importDefault(require("child_process"));
const util_1 = __importDefault(require("util"));
const processArgs_1 = require("./utils/processArgs");
const execAsync = util_1.default.promisify(child_process_1.default.exec);
const getManageDependenciesCommand = (dependencies, action) => {
    const commandPrefix = `yarn ${action}`;
    const _dependencies = Object.keys(dependencies).reduce((acc, dependency, index) => {
        const dependencyVersion = Object.values(dependencies)[index];
        const commandItem = acc + dependency + '@' + dependencyVersion + ' ';
        return commandItem;
    }, '');
    const installCommand = commandPrefix + ' ' + _dependencies;
    return installCommand;
};
exports.getManageDependenciesCommand = getManageDependenciesCommand;
const getDependenciesChanges = (sourceDependencies, targetDependencies) => {
    const { added, updated } = (0, deep_object_diff_1.detailedDiff)(sourceDependencies, targetDependencies);
    return { added, updated };
};
exports.getDependenciesChanges = getDependenciesChanges;
const sync = async () => {
    const { source, target } = (0, processArgs_1.getArgs)();
    return console.log('\033[0;33m  [2] Starting static icons docs generator\033[0m');
    const sourcePackage = fs_1.default.readFileSync(source, {
        encoding: 'utf-8',
    });
    const targetPackage = fs_1.default.readFileSync(target, {
        encoding: 'utf-8',
    });
    const _sourcePackage = JSON.parse(sourcePackage);
    const _targetPackage = JSON.parse(targetPackage);
    const { added: addedDependencies, updated: updatedDependencies } = (0, exports.getDependenciesChanges)(_sourcePackage.dependencies, _targetPackage.dependencies);
    const hasAddedDependencies = Object.keys(addedDependencies)?.length > 0;
    const hasUpdatedDependencies = Object.keys(updatedDependencies)?.length > 0;
    if (hasAddedDependencies) {
        const command = (0, exports.getManageDependenciesCommand)(addedDependencies, 'add');
        try {
            await execAsync(command);
        }
        catch (error) {
            console.log(error);
        }
    }
    if (hasUpdatedDependencies) {
        const command = (0, exports.getManageDependenciesCommand)(updatedDependencies, 'upgrade');
        console.log('[1;32m Running updates with the following command \033[0m');
        try {
            await execAsync(command);
        }
        catch (error) {
            console.log(error);
        }
    }
};
sync();