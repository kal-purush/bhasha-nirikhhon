import importCwd from 'import-cwd';
import Metalsmith from 'metalsmith';

import { OptionsInterface } from '../plugin';
import { isObject } from '../utils';

const PROP = 'plugins';
type ReturnValueType = OptionsInterface[typeof PROP];

/**
 * @see https://github.com/segmentio/metalsmith/blob/f9c5bd82701683ce648431e51ea2ee87ac07048f/bin/_metalsmith#L143-L161
 */
export function getPluginList(obj: unknown): Record<string, unknown>[] {
    if (Array.isArray(obj)) {
        const list: unknown[] = obj;
        return list.filter(isObject);
    }
    if (isObject(obj)) {
        return Object.entries(obj).map(([key, value]) => ({ [key]: value }));
    }
    throw new TypeError(
        `"${PROP}" option value must be an object or an array: ${typeof obj}`,
    );
}

export function isPlugin(value: unknown): value is Metalsmith.Plugin {
    return typeof value === 'function';
}

export function importFunc(filepath: string): Function {
    /**
     * @see https://github.com/nodejs/node/blob/v12.7.0/lib/internal/modules/cjs/loader.js#L677-L680
     */
    if (filepath === '') {
        throw new TypeError(
            `The plugin name for the "${PROP}" option must be a non-empty string`,
        );
    }

    let func: unknown;
    try {
        func = importCwd(filepath);
    } catch (err) {
        throw new TypeError(
            `Failed to import plugin "${filepath}" specified in option "${PROP}"`,
        );
    }

    if (typeof func !== 'function') {
        throw new TypeError(
            `Plugin "${filepath}" specified in option "${PROP}" did not export the function: ${typeof func}`,
        );
    }

    return func;
}

export function normalize(value: unknown): ReturnValueType {
    return getPluginList(value)
        .map(plugins =>
            Object.entries(plugins).map(([name, opts]) => {
                const pluginGenerator = importFunc(name);
                const plugin: unknown = pluginGenerator(opts);
                if (!isPlugin(plugin)) {
                    throw new TypeError(
                        `Plugin "${name}" specified in option "${PROP}" did not return the function: ${typeof plugin}`,
                    );
                }
                return plugin;
            }),
        )
        .reduce((pluginList1, pluginList2) => [...pluginList1, ...pluginList2]);
}