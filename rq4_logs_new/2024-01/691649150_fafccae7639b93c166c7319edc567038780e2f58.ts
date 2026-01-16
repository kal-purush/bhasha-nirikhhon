import { defineQuickFormat } from '@jujulego/quick-tag';
import path from 'node:path';
import process from 'node:process';

// Types
export interface QLogRelative {
  readonly from?: string;
}

/**
 * Prints relative path from given one. Default to relative path from `process.cwd()`
 */
export const qLogRelative = defineQuickFormat((arg: string, opts: QLogRelative = {}) => {
  const { from = process.cwd() } = opts;
  return path.relative(from, arg);
});