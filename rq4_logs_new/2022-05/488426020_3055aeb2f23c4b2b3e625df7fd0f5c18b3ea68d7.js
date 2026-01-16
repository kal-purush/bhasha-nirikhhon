module.exports = {
	env: {
		browser: true,
		commonjs: true,
		es2021: true,
	},
	extends: [
		'airbnb-base',
	],
	parserOptions: {
		ecmaVersion: 'latest',
	},
	rules: {
		indent: [
			'error',
			'tab',
		],
		'no-tabs': 0,
		semi: [
			'error',
			'never',
		],
	},
}