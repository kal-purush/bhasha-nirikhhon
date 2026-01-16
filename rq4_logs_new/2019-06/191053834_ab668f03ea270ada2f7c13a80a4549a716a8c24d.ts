import * as vscode from 'vscode';
import path from 'path';
import {
  formatAggregates,
  formatNewest,
  formatOldest,
  formatTodos,
} from '../../textUtils';
import { markdownFilesSearch, todosSearch } from '../search';
import { currentActiveFile, currentActiveWorkspace } from '.';

export const AGGREGATES_ACTION = 'AGGREGATES_ACTION';
export const NEWEST_ACTION = 'NEWEST_ACTION';
export const OLDEST_ACTION = 'OLDEST_ACTION';
export const LOCAL_TODOS_ACTION = 'LOCAL_TODOS_ACTION';
export const GLOBAL_TODOS_ACTION = 'GLOBAL_TODOS_ACTION';

const mapper = {
  [AGGREGATES_ACTION]: async () => {
    const matches = await markdownFilesSearch();
    return formatAggregates(matches);
  },
  [NEWEST_ACTION]: async () => {
    const matches = await markdownFilesSearch();
    return formatNewest(matches);
  },
  [OLDEST_ACTION]: async () => {
    const matches = await markdownFilesSearch();
    return formatOldest(matches);
  },
  [LOCAL_TODOS_ACTION]: async () => {
    const todos = await todosSearch(path.dirname(currentActiveFile()));
    return formatTodos(todos);
  },
  [GLOBAL_TODOS_ACTION]: async () => {
    const todos = await todosSearch(currentActiveWorkspace());
    return formatTodos(todos);
  },
};

export type Action = keyof typeof mapper;

export class MarkPlanTextDocumentContentProvider
  implements vscode.TextDocumentContentProvider {
  onDidChangeEmitter = new vscode.EventEmitter<vscode.Uri>();
  onDidChange = this.onDidChangeEmitter.event;

  async provideTextDocumentContent(uri: vscode.Uri): Promise<string> {
    const action = uri.path as Action;
    const fn = mapper[action];

    if (!fn) {
      return 'Unknown action';
    }

    try {
      const content = await fn();
      return `# ${action}\n\n${content}`;
    } catch (err) {
      return `Error: ${err.message}`;
    }
  }

  update(uri: vscode.Uri) {
    this.onDidChangeEmitter.fire(uri);
  }
}