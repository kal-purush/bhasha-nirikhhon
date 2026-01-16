import eslint from '@eslint/js';
import eslintConfigPrettier from 'eslint-config-prettier';
import eslintPluginSvelte from 'eslint-plugin-svelte';
import svelteParser from 'svelte-eslint-parser';
import tseslint from 'typescript-eslint';
import globals from 'globals';

export default tseslint.config(
	{
		extends: [eslint.configs.recommended, ...tseslint.configs.recommended],
		plugins: {
			'@typescript-eslint': tseslint.plugin
		},
		languageOptions: {
			parser: tseslint.parser,
			parserOptions: {
				sourceType: 'module',
				ecmaVersion: 2020,
				extraFileExtensions: ['.svelte']
			},
			globals: {
				...globals.browser,
				...globals.es2017,
				...globals.node
			}
		}
	},
	{
		files: ['**/*.svelte'],
		languageOptions: {
			parser: svelteParser,
			parserOptions: {
				parser: tseslint.parser,
				tsconfigRootDir: process.env.INIT_CWD,
				project: './tsconfig.json'
			}
		},
		rules: {
			'@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_$' }]
		}
	},
	...[
		...eslintPluginSvelte.configs['flat/recommended'],
		...eslintPluginSvelte.configs['flat/prettier']
	].map(({ rules, ...rest }) => ({
		// Workaround since svelte-eslint's typings are mismatched with ts-eslint's
		rules: {
			...rules
		},
		...rest
	})),
	eslintConfigPrettier,
	{
		ignores: ['*.cjs', '.svelte-kit/', '.turbo/', 'node_modules/']
	}
);