import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import {MonacoEditorModule, NgxMonacoEditorConfig} from 'ngx-monaco-editor';

const monacoConfig: NgxMonacoEditorConfig = {
  onMonacoLoad: () => {
    monaco.languages.register({id: 'mySpecialLanguage'});


// Register a tokens provider for the language
    monaco.languages.setMonarchTokensProvider('mySpecialLanguage', {
      tokenizer: {
        root: [
          [/\[error.*/, "custom-error"],
          [/\[notice.*/, "custom-notice"],
          [/\[info.*/, "custom-info"],
          [/\[[a-zA-Z 0-9:]+\]/, "custom-date"],
        ]
      }
    });


// Register a completion item provider for the new language
    monaco.languages.registerCompletionItemProvider('mySpecialLanguage', {
      provideCompletionItems: () => {
        var suggestions = [{
          label: 'simpleText',
          kind: monaco.languages.CompletionItemKind.Text,
          insertText: 'simpleText'
        }, {
          label: 'testing',
          kind: monaco.languages.CompletionItemKind.Keyword,
          insertText: 'testing(${1:condition})',
          insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
        }, {
          label: 'ifelse',
          kind: monaco.languages.CompletionItemKind.Snippet,
          insertText: [
            'if (${1:condition}) {',
            '\t$0',
            '} else {',
            '\t',
            '}'
          ].join('\n'),
          insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
          documentation: 'If-Else Statement'
        }];
        return {suggestions: suggestions};
      }
    });

    monaco.editor.create(document.getElementById("container"), {
      theme: 'myCoolTheme',
      value: getCode(),
      language: 'mySpecialLanguage'
    });

    function getCode() {
      return [
        '[Sun Mar 7 16:02:00 2004] [notice] Apache/1.3.29 (Unix) configured -- resuming normal operations',
        '[Sun Mar 7 16:02:00 2004] [info] Server built: Feb 27 2004 13:56:37',
      ].join('\n');
    }
  }}

@NgModule({
  imports: [
    CommonModule,
    MonacoEditorModule.forRoot(monacoConfig)

  ],
  declarations: [],
  exports: [MonacoEditorModule]
})
export class ConfEditor { }