import * as vscode from 'vscode';
import * as utils from './utils';
import * as relativePath from './relativePaths';

const copyRelPath = (res: Object, files: Array<vscode.Uri>) => {
  const workspace = vscode.workspace.getWorkspaceFolder(files[0]);
  if(workspace){
      const relPaths = relativePath.getRelPaths(files, workspace);
      if(relPaths){
          utils.copy(relPaths);
      }
  }
};

const copyRelPathFoc = (res: Object, files: Array<vscode.Uri>) => {
  const workspace = vscode.workspace.getWorkspaceFolder(files[0]);
  if(workspace){
      const relPaths = relativePath.getRelPaths(files, workspace);
      if(relPaths){
          const focusedRelativePath = relativePath.getFocusedRelativePaths(relPaths, workspace);
          if(focusedRelativePath) {
              utils.copy(focusedRelativePath);
          }
      }
  }
};

export {
  copyRelPath,
  copyRelPathFoc
};