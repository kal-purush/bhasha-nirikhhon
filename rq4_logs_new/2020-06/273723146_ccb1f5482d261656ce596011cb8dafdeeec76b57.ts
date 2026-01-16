import * as vscode from "vscode";

import {
  getAllModuleNames,
  createDavinciComponent,
  createDavinciModule,
} from "../utils";
import { DEFAULT_MODULE_NAME } from "../config";

const execScript = (componentName: string, selectedModuleName: string) => {
  const terminal = vscode.window.createTerminal();
  terminal.sendText(
    createDavinciComponent(componentName, selectedModuleName),
    true
  );
  terminal.show();
};

const createComponent = async () => {
  // Display a message box to the user
  const componentName = await vscode.window.showInputBox({
    prompt: "Enter component name",
  });

  if (!componentName) {
    vscode.window.showErrorMessage("Component name must be provided");
    return;
  }

  const moduleNames = getAllModuleNames();

  if (!moduleNames.length) {
    createDavinciModule(DEFAULT_MODULE_NAME);
    execScript(componentName, DEFAULT_MODULE_NAME);
    return;
  }

  const selectedModuleName = await vscode.window.showQuickPick(moduleNames);

  execScript(componentName, selectedModuleName || DEFAULT_MODULE_NAME);
};
export default createComponent;