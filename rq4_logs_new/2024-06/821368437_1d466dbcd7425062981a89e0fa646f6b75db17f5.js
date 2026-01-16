"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.deactivate = exports.activate = exports.checkPipModules = void 0;
const vscode = require("vscode");
const child_process_1 = require("child_process");
const child_process = require("child_process");
function checkPipModules(modules) {
    const outputArray = [];
    for (const module of modules) {
        try {
            const output = (0, child_process_1.execSync)(`pip3 show ${module}`).toString();
            const moduleInfo = output.split('\n');
            const moduleName = moduleInfo[0].split(':')[1].trim();
            const isInstalled = moduleInfo.some(info => info.includes('Location'));
            outputArray.push({ module: moduleName, installed: isInstalled });
        }
        catch (error) {
            outputArray.push({ module, installed: false });
        }
    }
    return outputArray;
}
exports.checkPipModules = checkPipModules;
function installPipModules(modules) {
    const modulesToInstall = modules.filter(module => !module.installed);
    if (modulesToInstall.length === 0) {
        console.log('All modules are already installed.');
        return;
    }
    const installedModules = child_process.execSync("pip3 list --format=columns | awk '{print $1}'").toString().split("\n").slice(2, -1);
    const modulesToCheck = modulesToInstall.filter(module => !installedModules.includes(module.module));
    if (modulesToCheck.length === 0) {
        console.log('All modules are already installed.');
        return;
    }
    const moduleNames = modulesToCheck.map(module => module.module);
    let errorModules = [];
    for (let i = 0; i < moduleNames.length; i++) {
        try {
            child_process.execSync(`pip3 install ${moduleNames[i]}`);
            console.log(`Successfully installed ${moduleNames[i]}.`);
        }
        catch (error) {
            if (error.message.includes("Could not find a version")) {
                errorModules.push(moduleNames[i]);
            }
            else {
                throw error;
            }
        }
    }
    if (errorModules.length > 0) {
        console.log(`Could not find a version for ${errorModules.join(', ')}, but continuing with the installation of other modules`);
    }
}
function activate(context) {
    let disposable = vscode.commands.registerCommand('pip-installer.scanImports', () => {
        let editor = vscode.window.activeTextEditor;
        if (!editor) {
            return;
        }
        let document = editor.document;
        let text = document.getText();
        const pythonModules = Array.from(new Set(text
            .split('\n')
            .filter(line => line.startsWith('import') || line.startsWith('from'))
            .map(line => line.split(' ')[1])
            .map(module => module.split('.')[0])));
        console.log(pythonModules);
        let modules = checkPipModules(pythonModules);
        console.log(checkPipModules(pythonModules));
        installPipModules(modules);
    });
    context.subscriptions.push(disposable);
}
exports.activate = activate;
function deactivate() { }
exports.deactivate = deactivate;
//# sourceMappingURL=extension-old.js.map