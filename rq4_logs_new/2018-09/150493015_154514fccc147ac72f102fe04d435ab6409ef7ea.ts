import * as yaml from 'js-yaml';
import * as fs from 'fs';

/**
 * Reads yml properties and inserts into the env
 * @param path yaml file location
 * @param propertyName property group from the yaml to be inserted into node process env
 */
export const yaml2env = (path: string, propertyName: string) => {
    try {
        const yamlConfig = yaml.safeLoad(fs.readFileSync(path, 'utf-8'));
        Object.keys(yamlConfig[propertyName]).forEach(k => {
            process.env[k] = yamlConfig[propertyName][k];
        });
    } catch (e) {
        console.error(e);
        throw e;
    }
};