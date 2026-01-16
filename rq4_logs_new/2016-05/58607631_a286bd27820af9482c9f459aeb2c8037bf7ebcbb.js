module.exports = {
    "env": {
        "browser": true,
        "node": true
    },
    "extends": "eslint:recommended",
    "rules": {
        "indent": [
            "error",
            4
        ],
        "linebreak-style": [
            "error",
            "windows"
        ],
        "quotes": [
            "error",
            "double"
        ],
        "semi": [
            "error",
            "always"
        ], 
        "comma-dangle": [ 
            "error",  
            "never" 
        ], 
        "no-undef": [ 
            "error" 
        ], 
            "no-unused-vars": [ 
            "error" 
        ], 
        "max-len": [ 
            "error",  
            99,  
            2, 
            {"ignoreUrls": true} 
        ], 
        "no-trailing-spaces": [ 
            "error",  
            { "skipBlankLines": true } 
        ], 
        "no-console": [ 
            "error" 
        ], 
         "eqeqeq": [ 
            "error",  
            "allow-null" 
        ], 
        "newline-before-return": [ 
            "error" 
        ], 
        "object-curly-spacing": [ 
            "error",  
            "always" 
        ]
    }
};