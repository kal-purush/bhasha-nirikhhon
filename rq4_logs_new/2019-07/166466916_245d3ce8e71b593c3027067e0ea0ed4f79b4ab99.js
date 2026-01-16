module.exports = {
  "root": true,
  "env": {
    "node": true,
    "es6": true
  },
  "parserOptions": {
    "ecmaVersion": 2018,
    "sourceType": "module"
  },
  "globals": {},
  "rules": {
    // generic eslint rules
    "no-alert": 0,
    "no-array-constructor": 0,
    "no-await-in-loop": 0,
    "no-bitwise": 0,
    "no-caller": 0,
    "no-case-declarations": 2, // disallow lexical declarations in case clauses
    "no-catch-shadow": 0,
    "no-class-assign": 2, //disallow reassigning class members
    "no-cond-assign": 2, // disallow assignment operators in conditional expressions
    "no-confusing-arrow": 0,
    "no-console": [2, {"allow": ["info", "warn", "error"]}], // disallow the use of console
    "no-const-assign": 2, // disallow reassigning const variables
    "no-constant-condition": 2, // disallow constant expressions in conditions
    "no-continue": 0,
    "no-control-regex": 2, // disallow control characters in regular expressions
    "no-debugger": 2, // disallow the use of debugger
    "no-delete-var": 2, // disallow deleting variables
    "no-div-regex": 0,
    "no-dupe-args": 2, // disallow duplicate arguments in function definitions
    "no-dupe-class-members": 2, // disallow duplicate class members
    "no-dupe-keys": 2, // disallow duplicate keys in object literals
    "no-duplicate-case": 2, // disallow duplicate case labels
    "no-duplicate-imports": 2, // disallow duplicate module imports
    "no-else-return": 2, // disallow else blocks after return statements in if statements
    "no-empty": 2, // disallow empty block statements
    "no-empty-character-class": 2, // disallow empty character classes in regular expressions
    "no-empty-function": 2, // disallow empty functions
    "no-empty-pattern": 2, // disallow empty destructuring patterns
    "no-eq-null": 2, // disallow null comparisons without type-checking operators
    "no-eval": 0,
    "no-ex-assign": 2, // disallow reassigning exceptions in catch clauses
    "no-extend-native": 0,
    "no-extra-bind": 2, // disallow unnecessary calls to .bind()
    "no-extra-boolean-cast": 2, // disallow unnecessary boolean casts
    "no-extra-label": 2, // disallow unnecessary labels
    "no-extra-parens": [2, "all", {"ignoreJSX": "multi-line"}], // disallow unnecessary parentheses
    "no-extra-semi": 2, // disallow unnecessary semicolons
    "no-fallthrough": 2, // disallow fallthrough of case statements
    "no-floating-decimal": 2, // disallow leading or trailing decimal points in numeric literals (.5, 2., -.7)
    "no-func-assign": 2, // disallow reassigning function declarations
    "no-global-assign": 2, // disallow assignment to native objects or read-only global variables
    "no-implicit-coercion": 2, // disallow shorthand type conversions
    "no-implicit-globals": 0,
    "no-implied-eval": 0,
    "no-inline-comments": 0,
    "no-inner-declarations": 2, // disallow variable or function declarations in nested blocks
    "no-invalid-regexp": 2, // disallow invalid regular expression strings in RegExp constructors
    "no-invalid-this": 0,
    "no-irregular-whitespace": 2, // disallow irregular whitespace outside of strings and comments
    "no-iterator": 0,
    "no-label-var": 0,
    "no-labels": 0,
    "no-lone-blocks": 2, // disallow unnecessary nested blocks
    "no-lonely-if": 2, // disallow if statements as the only statement in else blocks (can be replaced by else if)
    "no-loop-func": 0,
    "no-magic-numbers": 0,
    "no-mixed-operators": 0,
    "no-mixed-requires": 0,
    "no-mixed-spaces-and-tabs": 2, // disallow mixed spaces and tabs for indentation
    "no-multi-assign": 0,
    "no-multi-spaces": [2, {
      "ignoreEOLComments": true,
      exceptions: {
        "Property": true,
        "BinaryExpression": true,
        "VariableDeclarator": true,
        "ImportDeclaration": true
      }
    }], // aims to disallow multiple whitespace around logical expressions, conditional expressions, declarations, array elements, object properties, sequences and function parameters
    "no-multi-str": 0,
    "no-multiple-empty-lines": [2, {
      "max": 2,
      "maxEOF": 1,
      "maxBOF": 1
    }], // disallow multiple empty lines
    "no-negated-condition": 2, // disallow negated conditions in ternary expressions and if statements which have an else branch
    "no-nested-ternary": 0,
    "no-new": 0,
    "no-new-func": 0,
    "no-new-object": 0,
    "no-new-require": 0,
    "no-new-symbol": 2, // disallow new operators with the Symbol object
    "no-new-wrappers": 0,
    "no-obj-calls": 2, // disallows calling the Math, JSON and Reflect objects as functions
    "no-octal": 2, // disallow octal literals
    "no-octal-escape": 0,
    "no-param-reassign": 0,
    "no-path-concat": 0,
    "no-plusplus": 0,
    "no-process-env": 0,
    "no-process-exit": 0,
    "no-proto": 0,
    "no-prototype-builtins": 0,
    "no-redeclare": 2, // disallow variable redeclaration
    "no-regex-spaces": 2, // disallow multiple spaces in regular expressions
    "no-restricted-globals": 0,
    "no-restricted-imports": 0,
    "no-restricted-modules": 0,
    "no-restricted-properties": 0,
    "no-restricted-syntax": 0,
    "no-return-assign": 0,
    "no-return-await": 0,
    "no-script-url": 0,
    "no-self-assign": 2, // disallow assignments where both sides are exactly the same
    "no-self-compare": 2, // disallow comparisons where both sides are exactly the same
    "no-sequences": 0,
    "no-shadow": [0, {
      "builtinGlobals": false,
      "hoist": "functions",
      "allow": ["resolve", "reject", "done"]
    }], // disallow variable declarations from shadowing variables declared in the outer scope
    "no-shadow-restricted-names": 2, // disallow identifiers from shadowing restricted names
    "no-whitespace-before-property": 2, // disallow whitespace before properties
    "no-sparse-arrays": 2, // disallow sparse arrays (This rule disallows sparse array literals which have “holes” where commas are not preceded by elements. It does not apply to a trailing comma following the last element)
    "no-sync": 0,
    "no-tabs": 2, // disallow all tabs (This rule looks for tabs anywhere inside a file: code, comments or anything else)
    "no-ternary": 0,
    "no-trailing-spaces": 0,
    "no-this-before-super": 2, // disallow this/super before calling super() in constructors
    "no-throw-literal": 2, // disallow throwing literals as exceptions
    "no-undef": 2, // disallow the use of undeclared variables unless mentioned in /*global */ comments
    "no-undef-init": 0,
    "no-undefined": 0,
    "no-unexpected-multiline": 2, // disallow confusing multiline expressions
    "no-underscore-dangle": 0,
    "no-unmodified-loop-condition": 0,
    "no-unneeded-ternary": 2, // disallow ternary operators when simpler alternatives exist
    "no-unreachable": 2, // disallow unreachable code after return, throw, continue, and break statements
    "no-unsafe-finally": 2, // disallow control flow statements in finally blocks
    "no-unsafe-negation": 2, // disallow negating the left operand of relational operators
    "no-unused-expressions": 0,
    "no-unused-labels": 2, // disallow unused labels
    "no-unused-vars": 2, // disallow unused variables
    "no-use-before-define": 0,
    "no-useless-call": 2, // disallow unnecessary calls to .call() and .apply()
    "no-useless-computed-key": 2, // disallow unnecessary computed property keys in object literals
    "no-useless-concat": 2, // disallow unnecessary concatenation of literals or template literals
    "no-useless-constructor": 0,
    "no-useless-escape": 0,
    "no-useless-rename": 0,
    "no-useless-return": 0,
    "no-void": 0,
    "no-var": 2, // require let or const instead of var
    "no-warning-comments": 0,
    "no-with": 0,
    "array-bracket-spacing": [2, "never"], // enforce consistent spacing inside array brackets
    "array-callback-return": 0,
    "arrow-body-style": 0,
    "arrow-parens": [2, "as-needed"], // require parens in arrow function arguments (if they are needed)
    "arrow-spacing": [2, {
      "before": true,
      "after": true
    }], // enforce consistent spacing before and after the arrow in arrow functions
    "accessor-pairs": 0,
    "block-scoped-var": 0,
    "block-spacing": [2, "never"], // enforce consistent spacing inside single-line blocks
    "brace-style": [2, "1tbs", {"allowSingleLine": false}], // enforce consistent brace style for blocks
    "callback-return": 0,
    "camelcase": [2, {"properties": "never"}],
    "capitalized-comments": 0,
    "class-methods-use-this": 0,
    "comma-dangle": [2, "never"], // enforce consistent use of trailing commas in object and array literals
    "comma-spacing": 0,
    "comma-style": 0,
    "complexity": 0,
    "computed-property-spacing": [2, "never"], // enforce consistent spacing inside computed property brackets
    "consistent-return": 0,
    "consistent-this": 0,
    "constructor-super": 2, // require super() calls in constructors
    "curly": [2, "all"], // enforce consistent brace style for all control statements
    "default-case": 0,
    "dot-location": 0,
    "dot-notation": 2, // enforce dot notation whenever possible
    "eol-last": 0,
    "eqeqeq": 0,
    "func-call-spacing": 0,
    "func-names": 0,
    "func-name-matching": 0,
    "func-style": 0,
    "generator-star-spacing": 0,
    "global-require": 0,
    "guard-for-in": 0,
    "handle-callback-err": 0,
    "id-blacklist": 0,
    "id-length": 0,
    "id-match": 0,
    "indent": 0,
    "init-declarations": 0,
    "jsx-quotes": [2, "prefer-double"], // enforce the consistent use of either double or single quotes in JSX attributes
    "key-spacing": [2, {
      "beforeColon": false,
      "afterColon": true,
      "mode": "strict"
    }], // enforce consistent spacing between keys and values in object literal properties
    "keyword-spacing": [2, {
      "before": true,
      "after": true
    }], // enforce consistent spacing before and after keywords
    "linebreak-style": 0,
    "line-comment-position": 0,
    "lines-around-comment": 0,
    "lines-around-directive": 0,
    "max-depth": 0,
    "max-len": 0,
    "max-lines": 0,
    "max-nested-callbacks": 0,
    "max-params": 0,
    "max-statements": 0,
    "max-statements-per-line": 0,
    "multiline-ternary": 0,
    "new-cap": 0,
    "new-parens": 0,
    "newline-after-var": 0, // require or disallow an empty line after variable declarations
    "newline-before-return": 0,
    "newline-per-chained-call": 0,
    "object-curly-newline": 0, // enforce consistent line breaks inside braces
    "object-curly-spacing": [2, "never"], // enforce consistent spacing inside braces
    "object-property-newline": 0,
    "object-shorthand": [2, "properties"], // require or disallow method and property shorthand syntax for object literals
    "one-var": 0,
    "one-var-declaration-per-line": 0,
    "operator-assignment": 0,
    "operator-linebreak": 0,
    "padded-blocks": 0,
    "prefer-arrow-callback": 0,
    "prefer-const": 0, // require const declarations for variables that are never reassigned after declared
    "prefer-destructuring": [2, {
      "array": false,
      "object": true
    }], // require destructuring from arrays and/or objects
    "prefer-numeric-literals": 0,
    "prefer-promise-reject-errors": [2, {"allowEmptyReject": true}], // require using Error objects as Promise rejection reasons
    "prefer-rest-params": 0,
    "prefer-spread": 0,
    "prefer-template": 0,
    "quote-props": 0,
    "quotes": 0,
    "radix": 0,
    "require-await": 0,
    "require-jsdoc": 0,
    "require-yield": 2, // require generator functions to contain yield
    "rest-spread-spacing": 0,
    "semi": 2, // require or disallow semicolons instead of ASI
    "semi-spacing": 0,
    "sort-keys": 0,
    "sort-imports": 0,
    "sort-vars": 0,
    "space-before-blocks": 0,
    "space-before-function-paren": 0,
    "space-in-parens": [2, "never"], // enforce consistent spacing inside parentheses
    "space-infix-ops": 0,
    "space-unary-ops": 0,
    "spaced-comment": 0,
    "strict": 0,
    "symbol-description": 0,
    "template-curly-spacing": [2, "never"], // require or disallow spacing around embedded expressions of template strings
    "template-tag-spacing": 0,
    "unicode-bom": 0,
    "use-isnan": 2, // require calls to isNaN() when checking for NaN
    "valid-jsdoc": [2, {
      "requireReturn": false,
      "requireParamDescription": false,
      "requireReturnDescription": false
    }], // enforce valid JSDoc comments
    "valid-typeof": 2, // enforce comparing typeof expressions against valid strings
    "vars-on-top": 0,
    "wrap-iife": 0,
    "wrap-regex": 0,
    "no-template-curly-in-string": 2, // disallow template literal placeholder syntax in regular strings
    "yield-star-spacing": 0,
    "yoda": 0,

    // deprecated rules
    "no-native-reassign": 0,
    "no-negated-in-lhs": 0,
    "no-spaced-func": 0,
    "prefer-reflect": 0
  }
}