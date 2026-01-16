import { defineQuickFormat } from '@jujulego/quick-tag';
import { Log, LogLevel } from '../defs/index.js';

// Types
export interface QLogLevelOpts {
  /**
   * Ensure all log levels have the same size. Shorter ones will be appended with spaces.
   * @default true
   */
  fixLength?: boolean;
}

/**
 * Injects log's level name
 */
export const qlogLevel = defineQuickFormat((log: Log, opts: QLogLevelOpts = {}) => {
  const { fixLength = true } = opts;
  let level = LogLevel[log.level];

  if (fixLength && level.length < 7) {
    level += ' '.repeat(7 - level.length);
  }

  return level;
});