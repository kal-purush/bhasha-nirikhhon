import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv-safe';
import IConfig from 'interfaces/config';

dotenv.config({
    allowEmptyValues: true,
});

/**
 * Automatic load environment variables from configDir.
 * @param envsDir string
 * @returns IConfig
 */
function autoLoadEnvironmentVariables(envsDir: string): IConfig {
    const configDir = path.resolve(envsDir, 'configs');

    if (!fs.existsSync(configDir)) {
        throw new Error('configs dir does not exists');
    }

    const configFiles = fs.readdirSync(configDir);

    if (configFiles.length === 0) {
        throw new Error('does not exist any config files');
    }

    const configurations: any = [];

    configFiles.forEach((file) => {
        const filePath = path.join(configDir, file);
        configurations[file.split('.')[0]] = require(filePath).default;
    });

    return configurations;
}

export { autoLoadEnvironmentVariables };