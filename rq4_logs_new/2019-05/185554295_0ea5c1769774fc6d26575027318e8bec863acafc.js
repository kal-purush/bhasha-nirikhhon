	"prettier": {
		"useTabs": true,
		"semi": false,
		"singleQuote": false,
		"bracketSpacing": true,
		"trailingComma": "es5",
		"printWidth": 160
	},
	"eslintConfig": {
		"parserOptions": {
			"parser": "babel-eslint",
			"sourceType": "module"
		},
		"env": {
			"browser": true,
			"es6": true
		},
		"globals": {
			"Vue": true
		},
		"settings": {
			"import/resolver": {
				"webpack": {
					"config": "build/webpack.base.conf.js"
				}
			}
		},
		"extends": [
			"eslint:recommended",
			"plugin:vue/recommended"
		],
		"rules": {
			"quotes": [
				"error",
				"double",
				{
					"avoidEscape": true
				}
			],
			"vue/html-indent": "tab",
			"comma-dangle": [
				"error",
				{
					"arrays": "ignore",
					"objects": "always-multiline",
					"imports": "never",
					"exports": "never",
					"functions": "ignore"
				}
			],
			"arrow-parens": 0,
			"no-tabs": 0,
			"indent": [
				"error",
				"tab",
				{
					"SwitchCase": 1
				}
			],
			"no-console": 1,
			"generator-star-spacing": 0,
			"vue/max-attributes-per-line": [
				3,
				{
					"singleline": 3
				}
			],
			"vue/html-self-closing": [
				2,
				{
					"html": {
						"void": "never",
						"normal": "always",
						"component": "always"
					},
					"svg": "always",
					"math": "always"
				}
			]
		}
	}