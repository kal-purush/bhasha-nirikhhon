import * as vscode from 'vscode'; 

export function activate() { 
	console.log('DNX Watcher: Detected your "global.json" file correctly.');
	//Finding global.json
	vscode.workspace.findFiles("global.json","",1).then(function(arr){
		var globalJsonFile = arr[0];
		var currentSdkVersion=null;
		var getCurrentSdkVersion = function(){
			try{
				var json = JSON.parse(vscode.workspace.getTextDocument(globalJsonFile).getText());
				//Checking current version				
				currentSdkVersion=(json.sdk!=null?json.sdk.version:null);
				console.log('DNX Watcher: Detected SDK version '+currentSdkVersion);
			}
			catch(e){
				//Watching for malformed global.json
				currentSdkVersion=null;
				console.log('DNX Watcher: Failed to parse or read SDK version: '+e.message);
			}
		};
		
		// Initialize for first time version checking
		// getCurrentSdkVersion();
		
		//Watcher for file changes
		var watcher = vscode.workspace.createFileSystemWatcher(globalJsonFile.fsPath);		
		watcher.onDidChange(function(e){		
			console.log('DNX Watcher: "global.json" file changed');
			var localCurrentSdkVersion = currentSdkVersion;
			getCurrentSdkVersion();
			if(localCurrentSdkVersion != currentSdkVersion){
				vscode.commands.executeCommand("o.restart");
			}			
		});
	});	
	
}