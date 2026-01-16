import { PrettierOptions } from './typings/prettier';
import os from 'os';

const isWin = os.platform() === 'win32';
const options: PrettierOptions = {
    semi: true,
    singleQuote: true,
    tabWidth: 4,
    trailingComma: 'all',
    bracketSpacing: true,
    arrowParens: 'avoid',
    htmlWhitespaceSensitivity: 'ignore',
    printWidth: 80,
    endOfLine: isWin ? 'crlf' : 'lf',
    overrides: [
        {
            files: [
                // alphabetical order
                '.babelrc',
                '.czrc',
                '.eslintrc',
                '.prettierrc',
                '.releaserc',
                '.stylelintrc',
                '*.code-workspace',
            ],
            options: { parser: 'json' },
        },
        {
            files: [
                // alphabetical order
                'package.json',
                'package-lock.json',
                '*.md',
                '*.yml',
            ],
            options: { tabWidth: 2 },
        },
    ],
};

export = options;