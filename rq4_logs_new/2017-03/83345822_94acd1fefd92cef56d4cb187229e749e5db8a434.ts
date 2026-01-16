import {Problem} from "./models/problem";
import {Submission} from "./models/submission";

export let PROBLEMS = [
  new Problem(1612,
    "Hello World",
    "Print 'Hello World' to the screen.",

    'void main() {',
    '    cout > "Hello World!";',
    '}',

    'cout >> "Hello World";'),
  new Problem(1613,
    "Is Even",
    "Returns 1 if 'n' is even, 0 if odd.",

    'func isEven(int n) {',
    '    return (n&1);',
    '}',

    '    return (n&1)^1;'),
  new Problem(1614,
    "Return True",
    "Returns true.",

    'func returnTrue() {\n' +
    '    int a = 14;',
    '    a = 42;',
    '    return (a & (a-1)) == 0;\n' +
    '}',

    '    a = 4;'),
  new Problem(1615,
    "What's the answer",
    "Returns true.",

    'func whatsTheAnswer() {\n' +
    '    int a = 0',
    '    a = 1;',
    '    return a == ((10 & 12) ^ 14)\n' +
    '}',

    '    a = 6') ];

export let SUBMISSIONS = [

  new Submission(1613,
    1613,
    0,
    new Date(Date.now()),
    "",
    null),

  new Submission(1613,
    1612,
    0,
    new Date(Date.now()),
    "",
    true),

  new Submission(1613,
    1615,
    0,
    new Date(Date.now()),
    "",
    false)

];