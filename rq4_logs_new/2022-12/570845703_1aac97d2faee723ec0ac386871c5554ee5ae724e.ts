import {Config} from './index';
import jetpack from 'fs-jetpack';

export function handleAfterRun(config: Config) {
  return async (
    results:
      | CypressCommandLine.CypressRunResult
      | CypressCommandLine.CypressFailedRunResult
  ) => {
    const times =
      jetpack
        .read('times.txt')
        ?.split('\n')
        .map(time => time.trim())
        .map(Number) || [];
    const total = times.reduce((a, b) => a + b, 0);
    console.log(`Coverage plugin total added time: ${total}ms`);
  };
}