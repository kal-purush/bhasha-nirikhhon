import ConfigSource, { Value } from './ConfigSource';
import normalize from './normalize';

export default class Config {
  private keys = new Set<string>();

  public constructor(private sources: ConfigSource[]) {
    this.sources.reduce((keys, source) => [...keys, ...source.keys()], [])
      .forEach(key => this.keys.add(normalize(key)));
  }

  public get(key: string): Value {
    const found = this.findSourceAndKey(key);
    if (!found) {
      return null;
    }

    const value = found[0].get(found[1]);
    if (typeof value !== 'string') {
      return value;
    }

    return value.replace(
      /\${([^:}]+)(:([^}]*))?}/g,
      (_match, name, _p2, defaultValue) => this.get(name) || defaultValue,
    );
  }

  private findSourceAndKey(key: string): [ConfigSource, string] | null {
    const normalizedKey = normalize(key);
    for (const source of this.sources) {
      for (const x of source.keys()) {
        const normalizedX = normalize(x);
        if (normalizedX === normalizedKey) {
          return [source, x];
        }
      }
    }

    return null;
  }

  public extend<T>(obj: T, prefix = ''): T {
    return [...Object.keys(obj)]
      .reduce((prev, name) => Object.defineProperty(prev, name, {
        enumerable: true,
        get: () => {
          const key = normalize(`${prefix}${name}`);
          return this.keys.has(key)
            ? this.get(key)
            : this.extend((obj as any)[name], `${key}_`);
        },
      }), {});
  }
}