import React from "react";
import { Layout } from "../../components";

const code = `// values.js

// using addition (+) with strings concatenates them
console.log("node" + "js");

// you can print numbers directly to console without explicit type coercion
console.log("1 + 1 =", 1+1);
console.log("7.0 / 3.0 =", 7.0/3.0);

// booleans work as expected
console.log(true && false);
console.log(true || false);
console.log(!true);
`;

const output = `$ node values.js
nodejs
1 + 1 = 2
7.0 / 3.0 = 2.3333333333333335
false
true
false
`;

export default () => (
  <Layout title="Values" code={code} output={output}>
    <p>
      The first program you should write is the traditional "hello world"
      program, which lets you get to grips with the execution environment as
      well as basic program output.
    </p>
    <p>
      The console provides a way to debug programs by outputting data at various
      points during a programs execution.
    </p>
    <p>
      If you run <code>console.log</code> when writing JavaScript in the
      browser, you would see the output in the browser's console. In Node.js, we
      will simply see the output in the terminal.
    </p>
  </Layout>
);