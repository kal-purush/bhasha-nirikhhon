module.exports = {
  "env": {
    "browser": false,
    "node": true,
    "commonjs": true,
    "es6": true 
  },
  "extends": "eslint:recommended",
  "globals": {
      "Atomics": "readonly",
      "SharedArrayBuffer": "readonly"
  },
  "parserOptions": {
    "ecmaVersion": 2018
  },
  "rules": {
    "no-console": "off" ,
    // 所有引号使用双引号
    "quotes":[
      "error",
      "double"
    ],
    // 数组和对象键值对最后一个逗号， never参数：不能带末尾的逗号, always参数：必须带末尾的逗号，
    // always-multiline：多行模式必须带逗号，单行模式不能带逗号
    "comma-dangle": [1, "always-multiline"],
    // 禁用 alert、confirm 和 prompt
    "no-alert": 0,
    // 控制逗号前后的空格
    "comma-spacing": [2, {
      "before": false,
      "after": true
    }],
    // 控制逗号在行尾出现还是在行首出现 (默认行尾)
    // http://eslint.org/docs/rules/comma-style
    "comma-style": [2, "last"],
  }
};
//https://blog.csdn.net/weixin_36934930/article/details/90673863  规则