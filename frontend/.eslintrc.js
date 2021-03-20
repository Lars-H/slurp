module.exports = {
	root: true,
	reportUnusedDisableDirectives: true,
	parser: "@typescript-eslint/parser",
	plugins: ["@typescript-eslint", "import", "jest"],
	env: {
		browser: true,
		node: true,
		es6: true,
		"jest/globals": true,
	},
	ignorePatterns: ["src/customTypes"],
	extends: process.env.REACT_APP_DEV_DISABLE_ESLINT
		? []
		: [
				"plugin:react/recommended",
				"eslint:recommended",
				"plugin:@typescript-eslint/eslint-recommended",
				"plugin:@typescript-eslint/recommended",
				"plugin:jest/recommended",
				"plugin:jest/style",
				"plugin:import/typescript",
				// Enables eslint-plugin-prettier and eslint-config-prettier. This will display prettier errors
				// as ESLint errors. Make sure this is always the last configuration in the extends array.
				// "plugin:prettier/recommended",
		  ],
	rules: {
		"@typescript-eslint/explicit-function-return-type": "off",
		"@typescript-eslint/explicit-module-boundary-types": "off",
		"@typescript-eslint/no-empty-function": "off",
		"@typescript-eslint/no-empty-interface": "off",
		// "@typescript-eslint/no-unused-vars": ["error", { ignoreRestSiblings: true }],

		// TODO
		"@typescript-eslint/no-explicit-any": "off",
		"@typescript-eslint/no-unused-vars": "off",
		"react/display-name": "off",

		"@typescript-eslint/no-unnecessary-type-assertion": "off",
		"@typescript-eslint/no-use-before-define": "off",
		"react/prop-types": "off",
		"no-mixed-spaces-and-tabs": "off",
	},
};
