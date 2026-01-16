export const bank = [
  {
    title: "Valid Parenthesis",
    problem: "Given a string, return true if every open parenthesis '(' has a corresponding closed parenthesis ')'",
    examples: [
      [`"((Hello), (World))"`, true],
      [`"Hi george :)"`, false],
      [`"())"`, false],
      [`"()()"`, true],
      [`"(())"`, true],
      [`"(()"`, false],
      [`"(("`, false],
      [`"())("`, false],
      [`")()"`, false],
      [`"))()"`, false]
    ]
  },
  {
    title: "Reversing Carets",
    problem: "Your goal is to write a function that takes in a string as input and returns that string reversing whatever text is inside the caret marks.",
    examples: [
        [`"foo^bat^bar"`, `"footabbar"`],
        [`"^a^bcd^efgh^ijklm^nopqrs^tuvwxy^z^"`, `"zbcdsrqponijklmhgfetuvwxya"`]
    ]
  },
  {
    title: "Spell Check",
    problem: `Implement a function to determine if a word is spelled correctly, and if it is not spelled correctly return the top options for the correct spelling of the intended word. You can use the following as a dictionary.`,
    reference:
`["family", "can", "cab", "cave", "it", "ant", "bit", "bait"]`,
    examples: [
      [`"family"`, `["family"]`],
      [`"faily"`, `["family"]`],
      [`"fammily"`, `["family"]`],
      [`"faxily"`, `["family"]`],
      [`"it"`, `["it", "bit"]`],
      [`""`, `[""]`],
    ],
  },
  {
    title: "Telephone",
    problem: "Given a number, return the list of strings that could be texted if using a dialer. The following letters can be printed per number.",
    reference: 
`0 -> 
1 -> 
2 -> a, b, c
3 -> d, e, f
4 -> g, h, i
5 -> j, k, l
6 -> m, n, o
7 -> p, q, r, s
8 -> t, u, v
9 -> w, x, y, z
`,
    examples: [
        [`123`, `["ad", "ae", "af", "bd", "be", "bf", "cd", "ce", "cf"]`],
        [`"a!"`, `NaN`],
        [`"000"`, `""`],
        [`""`, `""`],
    ]
  },
  {
    title: "S Number",
    problem: "A number qualifies as an S nunber if it's squareroot equals some permutation of it's numbers.",
    reference: `Explanation: sqrt(9801) === 98 + 0 + 1 === 99`,
    examples: [
        [`9801`, `true`],
        [`8281`, `true`],
        [`6724`, `true`],
    ]
  },
  {
    title: "Encoding",
    problem: "Write a function that takes in a string (only a-z, lowercase) and returns a compressed string with repeated characters replaced by a single instance follow by a number.",
    examples: [
        [`"a"`, `"a"`],
        [`"aa"`, `"aa"`],
        [`"aaaa"`, `"a4"`],
        [`"baaad"`, `"ba3d"`],
        [`"badbadbad"`, `"bad3"`],
        [`"badbeebad"`, `"badbeebad"`],
    ]
  },
  {
    title: "Highest Number",
    problem: "Write a function that takes a list of numbers, and returns the largest number in the list. If there are no numbers in the list, return 0.",
    examples: [
        [`[7, 2, 6, 3]`, `7`],
        [`[]`, `0`]
    ]
  },
  {
    title: "Word Wrap & Justify",
    problem: "Given a string and line limit, write a function that can wrap words into an array of strings with a set max length and adds hyphens to be exactly the set length., ",
    reference: `The day began as still as the night abruptly lighted with brilliant flame`,
    examples: [
        [`"wrap", 24`, `["The day began as still", "as the night abruptly", "lighted with brilliant", "flame"]`],
        [`"justify", 24`, `["The--day--began-as-still", "as--the--night--abruptly", "lighted--with--brilliant", "flame"]`],
        [`"wrap", 25`, `["The day began as still as", "the night abruptly", "lighted with brilliant", "flame"]`],
        [`"justify", 25`, `["The-day-began-as-still-as", "the-----night----abruptly", "lighted---with--brilliant", "flame"]`],
        [`"wrap", 40`, `["The day began as still as the night", "abruptly lighted with brilliant flame"]`],
        [`"justify", 40`, `["The--day--began--as--still--as-the-night", "abruptly--lighted--with--brilliant-flame"]`],
    ]
  },
  {
    title: "MinStack",
    problem: "Write a stack class with the following methods: push, pop, peek. Push adds to the top of the stack. Pop removes top item from stack and returns it. Peek inspects top item and returns it, but does not remove it. Implement a new method min, that returns the minimum item in the stack. The min method should be constant.",
    examples: [
        [`"minStack.min.pop()"`, `"???"`],
    ]
  },
  {
    title: "Fibonacci Sequence",
    problem: "Write a function that given a number n, returns the nth Fibonacci number.",
    examples: [
      [`"0"`, `"0"`],
      [`"1"`, `"1"`],
      [`"2"`, `"1"`],
      [`"3"`, `"2"`],
      [`"4"`, `"3"`],
      [`"5"`, `"5"`],
      [`"6"`, `"8"`],
      [`"7"`, `"13"`],
      [`"15"`, `"610"`],
      [`"50"`, `"12586269025"`]
    ]
  },
]