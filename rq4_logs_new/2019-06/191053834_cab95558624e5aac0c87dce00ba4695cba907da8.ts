import * as vscode from 'vscode';
import groupBy from 'lodash/groupBy';
import orderBy from 'lodash/orderBy';
import chunk from 'lodash/chunk';

import distanceInWordsStrict from 'date-fns/distance_in_words_strict';
import { search, SearchItem } from './modules/globby';

export const AGGREGATES_ACTION = 'AGGREGATES_ACTION';
export const NEWEST_ACTION = 'NEWEST_ACTION';
export const OLDEST_ACTION = 'OLDEST_ACTION';

const mapper = {
  [AGGREGATES_ACTION]: fetchAggregates,
  [NEWEST_ACTION]: fetchNewest,
  [OLDEST_ACTION]: fetchOldest,
};

export class MarkPlanTextDocumentContentProvider implements vscode.TextDocumentContentProvider {
  onDidChangeEmitter = new vscode.EventEmitter<vscode.Uri>();
  onDidChange = this.onDidChangeEmitter.event;

  async provideTextDocumentContent(uri: vscode.Uri): Promise<string> {
    const path = uri.path as keyof typeof mapper;
    const fn = mapper[path];

    if (!fn) {
      return 'Unknown action';
    }

    try {
      const content = await fn();
      return `# ${path}\n\n${content}`;
    } catch (err) {
      return `Error: ${err.message}`;
    }
  }
}

async function fetchAggregates() {
  const matches = await search();

  const targets = matches
    .filter(match => match.path.includes('--'))
    .map(item => {
      const match = /--([\w-]+)(\.|$)/g.exec(item.path);

      if (!match) {
        throw new Error(`Can't parse /${item.path}`);
      }

      item.group = match[1];
      return item;
    });

  const groups = groupBy(targets, 'group');

  return Object.entries(groups)
    .map(([group, items]) => {
      return `## ${group}\n\n${formatFiles(items)}`;
    })
    .join('\n\n');
}

async function fetchNewest() {
  return fetchOrdered('desc');
}

async function fetchOldest() {
  return fetchOrdered('asc');
}

async function fetchOrdered(order: 'asc' | 'desc') {
  const matches = await search();
  const targets = orderBy(matches, 'mtime', order);

  return chunk(targets.slice(0, 50), 5).map(items => formatFiles(items)).join('\n\n');
}

function formatFiles(items: Array<SearchItem>) {
  return items
    .map(
      target =>
        `- /${target.path} (${distanceInWordsStrict(
          target.mtime,
          Date.now()
        )})`
    )
    .join('\n');
}