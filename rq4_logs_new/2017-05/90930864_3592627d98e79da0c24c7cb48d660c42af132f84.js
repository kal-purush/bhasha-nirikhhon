// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
var vscode = require('vscode');
var Client = require('ssh2').Client;
var fs = require('fs');

function isWorkspaceOpen() {
	return vscode.workspace.rootPath !== undefined;
}

function getFilePath() {
  let filePath = '';
  if (vscode.workspace.rootPath !== undefined) {
    filePath = vscode.workspace.rootPath + '/ftp-settings.json';
  }
  return filePath;
}

// this method is called when your extension is activated
// your extension is activated the very first time the command is executed
function activate(context) {

	// Use the console to output diagnostic information (console.log) and errors (console.error)
	// This line of code will only be executed once when your extension is activated
	console.log('Congratulations, your extension "ftp-code" is now active!');
	var conn = new Client();

	var initSettings = vscode.commands.registerCommand('extension.initSettings', function() {
    if (!isWorkspaceOpen()) return;

    const filePath = getFilePath();
    const exists = fs.existsSync(filePath);
    if (exists) {
      vscode.window.showErrorMessage('Settings file already exist!');
    } else {
      const settings = JSON.stringify({
				"host": "",
				"port": "",
				"username": "",
				"password": "",
        "permissions": "",
        "path": ""
			});
      const fd = fs.openSync(filePath, 'wx');
      fs.writeFileSync(filePath, settings);
      fs.closeSync(fd);
    }
	});

	var ftpCheckCommand = vscode.commands.registerCommand('extension.checkConnection', function () {
    if (!isWorkspaceOpen()) return;

    const filePath = getFilePath();
    const exists = fs.existsSync(filePath);
    if (!exists) {
      vscode.window.showErrorMessage('Settings file does not exist!');
    } else {
      const settings = JSON.parse(fs.readFileSync(filePath));

      conn.on('ready', function() {
			  console.log('Connection Established');
		  }).connect(settings);
    }
	});

	var ftpChangePermissions = vscode.commands.registerCommand('extension.changePermissions', function () {
    if (!isWorkspaceOpen()) return;

    const filePath = getFilePath();
    const exists = fs.existsSync(filePath);
    if (!exists) {
      vscode.window.showErrorMessage('Settings file does not exist!');
    } else {
      const settings = JSON.parse(fs.readFileSync(filePath));
      settings.permissions = (settings.permissions.length !== 0) ? settings.permissions : '644';

      const activeTextEditor = vscode.window.activeTextEditor;
      if (activeTextEditor === undefined) {
        vscode.window.showErrorMessage('No active file in text editor!');
      } else {
        const fileName = activeTextEditor.document.fileName.split('/').splice(-1)[0];
        conn.on('ready', function() {
          conn.sftp(function(err, sftp) {
            if (err) throw err;
            sftp.chmod(settings.path + '/' + fileName, settings.permissions, function(err) {
              if (err) throw err;
              conn.end();
            });
          });
		    }).connect(settings);
      }
    }    
	});

  var ftpStat = vscode.commands.registerCommand('extension.stat', function(err) {

  });

	context.subscriptions.push(initSettings);
	context.subscriptions.push(ftpChangePermissions);
	context.subscriptions.push(ftpCheckCommand);
}
exports.activate = activate;

// this method is called when your extension is deactivated
function deactivate() {
	console.log('test');
}
exports.deactivate = deactivate;
