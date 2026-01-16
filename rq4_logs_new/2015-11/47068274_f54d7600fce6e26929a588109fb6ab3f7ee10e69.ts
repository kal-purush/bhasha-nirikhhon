// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode'; 

// this method is called when your extension is activated
// your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {
	let edPos = new EditPosition();

	// The command has been defined in the package.json file
	// Now provide the implementation of the command with  registerCommand
	// The commandId parameter must match the command field in package.json
	let disposable = vscode.commands.registerTextEditorCommand('navigate.gotoLastEdit', () => {
		// The code you place here will be executed every time your command is executed
		edPos.gotoPrevious();
	});
	
	let controller = new ExtController(edPos);
	
	context.subscriptions.push(disposable);
	context.subscriptions.push(edPos);
	context.subscriptions.push(controller);
}

class ExtController {
	private edPos: EditPosition;
	private disposable: vscode.Disposable;
	
	constructor(edPos: EditPosition) {
		this.edPos = edPos;
		let subscriptions: vscode.Disposable[] = [];
        vscode.workspace.onDidChangeTextDocument(this.onDocChanged, this, subscriptions);
        // vscode.window.onDidChangeActiveTextEditor(this._onEvent, this, subscriptions);
		vscode.window.onDidChangeTextEditorSelection(this.onSelectionChanged, this, subscriptions);
		let disp1 = vscode.languages.registerDocumentSymbolProvider('*', new DocSymbolProvider());
		subscriptions.push(disp1);
		this.disposable = vscode.Disposable.from(...subscriptions);
	}
	
	dispose() {
		this.disposable.dispose()
	}
	
	private onDocChanged({document, contentChanges}: vscode.TextDocumentChangeEvent) {
		var pos = contentChanges[0].range.start
		this.edPos.onNewEdit(document.fileName, pos);
	}
	
	private onSelectionChanged({selections, textEditor}: vscode.TextEditorSelectionChangeEvent) {
		this.edPos.onNewPosition(textEditor.document.fileName, selections[0].active);
	}
}

interface SymbolRule {re: RegExp; label?: string; kind?: string; ext?: string[]; excludeRe?: RegExp;};
class DocSymbolProvider {
	private rules: SymbolRule[] = [{re: /(?:#|\/\/|--)[ \t]*((\w+):.*)/}];
	
	constructor() {
		if(!vscode.workspace.rootPath) return;
		let file = vscode.Uri.file(vscode.workspace.rootPath + '/.symbol-rules');
		vscode.workspace.openTextDocument(file).then((document)=>{
			console.log('file found');
			
			for(var line = 0, lineCount = document.lineCount; line < lineCount; line++) {
				var lineText = document.lineAt(line);
				let rule = this.parseRule(lineText.text);
				if(rule) this.rules.push(rule);
			}
		},
		(err)=>{
			// console.log('.symbol-rules', err);
		});
	}
	
	parseRule(lineText: string) {
		// regex||kind||label||ext=
		function toRegex(str: string) {
			let match = str.match(/\/(.*)\/(i?)$/);
			return new RegExp(match[1], match[2]);
		}
		let rule: SymbolRule = {re: null};
		if(!lineText) return;
		let ruleStr = lineText.split('#rule:')[1];
		ruleStr = ruleStr && ruleStr.trim();
		if(!ruleStr) return;
		let parts = ruleStr.split('||');
		parts.forEach((part)=>{
			if(part.indexOf('//') == 0) {
				// secondary regex. Not used at the moment
				let re = toRegex(part.substr(1));
			}
			else if(part.indexOf('/') == 0) {
				rule.re = toRegex(part);
			}
			else if(part.indexOf('%')) {
				rule.label = part;
			}
			else if(part.indexOf('ext=')) {
				rule.ext = part.substr('ext='.length).split(',');
			}
			else if(part.match(/^(Array|Boolean|Class|Constant|Constructor|Enum|Field|File|Function|Interface|Method|Module|Namespace|Number|Package|Property|String|Variable)$/)) {
				rule.kind = part;
			}
		});
		if(rule.re === null) return;
		return rule;
	}
	
	rulesForFile(fileName: string) {
		let rules = this.rules;
		let activeRules: SymbolRule[] = [];
		for(let i = 0, len = rules.length; i < len; i++) {
			if(rules[i].ext) {
				for(let j = 0, lenJ = rules[i].ext.length; j < lenJ; j++) {
					let foundAt = fileName.lastIndexOf(rules[i].ext[j]);
					if(foundAt && foundAt + rules[i].ext[j].length === fileName.length) {
						activeRules.push(rules[i]);
						break;
					}
				}
			}
			else {
				activeRules.push(rules[i]);
			}
		}
		return activeRules;
	}
	
	provideDocumentSymbols(document: vscode.TextDocument, token: vscode.CancellationToken) {
		var activeRules = this.rulesForFile(document.fileName);
		var symInfos: vscode.SymbolInformation[] = [];
		for(var lineNum = 0, lineCount = document.lineCount; lineNum < lineCount; lineNum++) {
			var line = document.lineAt(lineNum);
			var lineText = line.text
			if(lineText.indexOf('#rule:') >= 0) {
				let rule = this.parseRule(line.text);
				activeRules.push(rule);
				continue;
			}
			activeRules.forEach( (rule)=>{
				let label: string;
				let match = lineText.match(rule.re);
				if(!match) return;
				// #todo: Fix symbol info to use specified label.
				if(rule.label) {
					label = rule.label.replace(/%(\d)/g, (ignore, p1)=>{
						return match[p1];
					});
				}
				else {
					label = match[1] || match[0]
				}
				let kind = rule.kind || 'Namespace'
				let info = new vscode.SymbolInformation(label, vscode.SymbolKind[kind], new vscode.Range(lineNum,0,lineNum,0)); 
				symInfos.push(info);
			});
		}
		return symInfos;
	}
}
	
enum EditPositionState {
	idle, 
	waiting, // Detected edit, waiting for position
	changing  // Changing position - this will cause one event
}

class EditPosition {
	private states : {[fileName: string] : EditPositionState} = {};
	private multiFilePositions : {fileName: string, position: vscode.Position}[] = [];
	private filePositions : {[fileName: string] : vscode.Position[]} = {};
	private multiIndex: number = -1;
	private fileIndex: number = -1;
	private editStartPosition;
	
		
	constructor() {
	}
	
	dispose() {
		
	}
	
	private updatePositions(fileName: string, startLine: number, endLine: number) {
		// Adjust line numbers
		var linesInserted = endLine - startLine;	// Can be negative	
		this.multiFilePositions.forEach( (filePos) => {
			if(filePos.fileName === fileName) {
				if(filePos.position.line >= startLine) {
					filePos.position = filePos.position.translate(linesInserted);
				}
			}
		});
		if(this.filePositions[fileName]) {
			this.filePositions[fileName].forEach((position, index)=>{
				if(position.line >= startLine) {
					position.line += linesInserted;
					this.filePositions[fileName][index] = position.translate(linesInserted);
				}
			})
		} 
		
	}
	
	gotoPrevious(fileName?: string) {
		 if(!fileName && this.multiFilePositions.length) {
			 if(this.multiIndex < 0){
			 	this.multiIndex =  this.multiFilePositions.length - 1
			 }
			 let {fileName, position} = this.multiFilePositions[this.multiIndex--];
			 let selection = new vscode.Selection(position, position);
			 vscode.workspace.openTextDocument(fileName).then((document) => {
				 vscode.window.showTextDocument(document).then((editor) => {
					 this.states[fileName] = EditPositionState.changing
					 editor.selection = selection
				 }, ()=>{})
			 }, ()=>{})
		 }
	}
	
	onNewEdit(fileName: string, position: vscode.Position) {
		this.editStartPosition = position;
		this.states[fileName] = EditPositionState.waiting;
	}
	
	onNewPosition(fileName: string, position: vscode.Position) {
		var prevState = this.states[fileName]
		this.states[fileName] = EditPositionState.idle
		if(prevState == EditPositionState.waiting) {
			if(this.editStartPosition.line != position.line) {
				// Edit has added or removed lines
				this.updatePositions(fileName, this.editStartPosition.line, position.line);
			}
			this.multiFilePositions.push({fileName, position});
			if(this.filePositions[fileName] == undefined) {
				this.filePositions[fileName] = []
			}
			this.filePositions[fileName].push(position);
		}
		else if(prevState == EditPositionState.changing) {
			return;
		}
		else {
			this.multiIndex = -1;
			this.fileIndex = -1;
		}
	}
}