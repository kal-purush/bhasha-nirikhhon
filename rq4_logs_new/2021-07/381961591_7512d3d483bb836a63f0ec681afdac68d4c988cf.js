const jsQuiz = {
  quizName: "JavaScript",
  quizId: 1,
  quizCardIcon:
    "https://res.cloudinary.com/hntejas/image/upload/v1621609210/quizzer/js-icon.png",
  questions: [
    {
      questionId: 1,
      question:
        "Which statement is the correct way to create a variable called rate and assign it the value 100?",
      options: [
        {
          optionId: 1,
          optionText: "let rate = 100;",
        },
        {
          optionId: 2,
          optionText: "let 100 = rate;",
        },
        {
          optionId: 3,
          optionText: "100 = let rate;",
        },
        {
          optionId: 4,
          optionText: "rate = 100;",
        },
      ],
    },
    {
      questionId: 2,
      question: "Which method converts JSON data to a JavaScript object?",
      options: [
        {
          optionId: 1,
          optionText: "JSON.fromString();",
        },
        {
          optionId: 2,
          optionText: "JSON.parse()",
        },
        {
          optionId: 3,
          optionText: "JSON.toObject()",
        },
        {
          optionId: 4,
          optionText: "JSON.stringify()",
        },
      ],
    },
    {
      questionId: 3,
      question:
        "Which Object method returns an iterable that can be used to iterate over the properties of an object?",
      options: [
        {
          optionId: 1,
          optionText: "Object.get()",
        },
        {
          optionId: 2,
          optionText: "Object.loop()",
        },
        {
          optionId: 3,
          optionText: "Object.each()",
        },
        {
          optionId: 4,
          optionText: "Object.keys()",
        },
      ],
    },
    {
      questionId: 4,
      question: "Output of 0 && hi",
      options: [
        {
          optionId: 1,
          optionText: "ReferenceError",
        },
        {
          optionId: 2,
          optionText: "True",
        },
        {
          optionId: 3,
          optionText: "0",
        },
        {
          optionId: 4,
          optionText: "false",
        },
      ],
    },
    {
      questionId: 5,
      question:
        "Which of the following operators can be used to do a short-circuit evaluation?",
      options: [
        {
          optionId: 1,
          optionText: "++",
        },
        {
          optionId: 2,
          optionText: "--",
        },
        {
          optionId: 3,
          optionText: "==",
        },
        {
          optionId: 4,
          optionText: "||",
        },
      ],
    },
    {
      questionId: 6,
      question:
        "Why would you include a 'use strict' statement in a JavaScript file?",
      options: [
        {
          optionId: 1,
          optionText:
            "to tell parsers to interpret your JavaScript syntax loosely",
        },
        {
          optionId: 2,
          optionText:
            "to tell parsers to enforce all JavaScript syntax rules when processing your code",
        },
        {
          optionId: 3,
          optionText:
            "to instruct the browser to automatically fix any errors it finds in the code",
        },
        {
          optionId: 4,
          optionText: "to enable ES6 features in your code",
        },
      ],
    },
    {
      questionId: 7,
      question: "Which of the following is not a keyword in JavaScript?",
      options: [
        {
          optionId: 1,
          optionText: "this",
        },
        {
          optionId: 2,
          optionText: "catch",
        },
        {
          optionId: 3,
          optionText: "function",
        },
        {
          optionId: 4,
          optionText: "array",
        },
      ],
    },
    {
      questionId: 8,
      question:
        "What is the name of a function whose execution can be suspended and resumed at a later point?",
      options: [
        {
          optionId: 1,
          optionText: "Generator function",
        },
        {
          optionId: 2,
          optionText: "Arrow function",
        },
        {
          optionId: 3,
          optionText: "Async/ Await function",
        },
        {
          optionId: 4,
          optionText: "Promise function",
        },
      ],
    },
    {
      questionId: 9,
      question:
        "Which collection object allows unique value to be inserted only once?",
      options: [
        {
          optionId: 1,
          optionText: "Object",
        },
        {
          optionId: 2,
          optionText: "Set",
        },
        {
          optionId: 3,
          optionText: "Array",
        },
        {
          optionId: 4,
          optionText: "Map",
        },
      ],
    },
    {
      questionId: 10,
      question:
        "If you attempt to call a value as a function but the value is not a function, what kind of error would you get?",
      options: [
        {
          optionId: 1,
          optionText: "TypeError",
        },
        {
          optionId: 2,
          optionText: "SystemError",
        },
        {
          optionId: 3,
          optionText: "SyntaxError",
        },
        {
          optionId: 4,
          optionText: "LogicError",
        },
      ],
    },
  ],
  answers: [
    {
      questionId: 1,
      correctOptionId: 1,
      explanation: "",
    },
    {
      questionId: 2,
      correctOptionId: 2,
      explanation: "",
    },
    {
      questionId: 3,
      correctOptionId: 4,
      explanation: "",
    },
    {
      questionId: 4,
      correctOptionId: 3,
      explanation: "",
    },
    {
      questionId: 5,
      correctOptionId: 4,
      explanation: "",
    },
    {
      questionId: 6,
      correctOptionId: 2,
      explanation: "",
    },
    {
      questionId: 7,
      correctOptionId: 4,
      explanation: "",
    },
    {
      questionId: 8,
      correctOptionId: 1,
      explanation: "",
    },
    {
      questionId: 9,
      correctOptionId: 2,
      explanation: "",
    },
    {
      questionId: 10,
      correctOptionId: 1,
      explanation: "",
    },
  ],
};

const reactQuiz = {
  quizName: "React",
  quizId: 2,
  quizCardIcon:
    "https://res.cloudinary.com/hntejas/image/upload/v1621609370/quizzer/react-icon.png",
  questions: [
    {
      questionId: 1,
      question: "What is the children prop?",
      options: [
        {
          optionId: 1,
          optionText: "a property that adds child components to state",
        },
        {
          optionId: 2,
          optionText:
            "a property that lets you pass components as data to other components",
        },
        {
          optionId: 3,
          optionText: "a property that lets you set an array as a property",
        },
        {
          optionId: 4,
          optionText: "a property that lets you pass data to child elements",
        },
      ],
    },
    {
      questionId: 2,
      question: "What can you use to handle code splitting?",
      options: [
        {
          optionId: 1,
          optionText: "React.memo",
        },
        {
          optionId: 2,
          optionText: "React.split",
        },
        {
          optionId: 3,
          optionText: "React.lazy",
        },
        {
          optionId: 4,
          optionText: "React.fallback",
        },
      ],
    },
    {
      questionId: 3,
      question:
        "A representation of a user interface that is kept in memory and is synced with the 'real' DOM is called what?",
      options: [
        {
          optionId: 1,
          optionText: "virtual DOM",
        },
        {
          optionId: 2,
          optionText: "DOM",
        },
        {
          optionId: 3,
          optionText: "virtual elements",
        },
        {
          optionId: 4,
          optionText: "shadow DOM",
        },
      ],
    },
    {
      questionId: 4,
      question:
        "What do you call a React component that catches JavaScript errors anywhere in the child component tree?",
      options: [
        {
          optionId: 1,
          optionText: "error catchers",
        },
        {
          optionId: 2,
          optionText: "error helpers",
        },
        {
          optionId: 3,
          optionText: "error bosses",
        },
        {
          optionId: 4,
          optionText: "error boundaries",
        },
      ],
    },
    {
      questionId: 5,
      question:
        "Why is it a good idea to pass a function to setState instead of an object?",
      options: [
        {
          optionId: 1,
          optionText: "It provides better encapsulation.",
        },
        {
          optionId: 2,
          optionText: "It makes sure that the object is not mutated.",
        },
        {
          optionId: 3,
          optionText: "It automatically updates a component.",
        },
        {
          optionId: 4,
          optionText:
            "setState is asynchronous and might result in out of sync values.",
        },
      ],
    },
  ],
  answers: [
    {
      questionId: 1,
      correctOptionId: 2,
      explanation: "",
    },
    {
      questionId: 2,
      correctOptionId: 3,
      explanation: "",
    },
    {
      questionId: 3,
      correctOptionId: 1,
      explanation: "",
    },
    {
      questionId: 4,
      correctOptionId: 4,
      explanation: "",
    },
    {
      questionId: 5,
      correctOptionId: 4,
      explanation: "",
    },
  ],
};

const htmlQuiz = {
  quizName: "HTML",
  quizId: 3,
  quizCardIcon:
    "https://res.cloudinary.com/hntejas/image/upload/v1621609370/quizzer/html-icon.png",
  questions: [
    {
      questionId: 1,
      question: "What is the primary purpose of the <canvas> tag?",
      options: [
        {
          optionId: 1,
          optionText: "It allows raster images to be rendered on the webpage.",
        },
        {
          optionId: 2,
          optionText: "It displays annotated images.",
        },
        {
          optionId: 3,
          optionText: "It allows drawing on a bitmap via JavaScript.",
        },
        {
          optionId: 4,
          optionText: "It allows vector images to be rendered on the webpage.",
        },
      ],
    },
    {
      questionId: 2,
      question: "When is the <link> tag used?",
      options: [
        {
          optionId: 1,
          optionText:
            "When linking style sheets, JavaScript, and icons for mobile apps",
        },
        {
          optionId: 2,
          optionText:
            "when linking style sheets, favicons, and preloading assets",
        },
        {
          optionId: 3,
          optionText: "when linking style sheets and favicons",
        },
        {
          optionId: 4,
          optionText: "when linking style sheets, external URLs, and favicons",
        },
      ],
    },
    {
      questionId: 3,
      question: "What is NOT a valid attribute for the <textarea> element?",
      options: [
        {
          optionId: 1,
          optionText: "readonly",
        },
        {
          optionId: 2,
          optionText: "max",
        },
        {
          optionId: 3,
          optionText: "form",
        },
        {
          optionId: 4,
          optionText: "spellcheck",
        },
      ],
    },
    {
      questionId: 4,
      question: "What is the semantic meaning of the <hr> tag?",
      options: [
        {
          optionId: 1,
          optionText: "It draws a horizontal line.",
        },
        {
          optionId: 2,
          optionText: "This tag is deprecated and should not be used.",
        },
        {
          optionId: 3,
          optionText:
            "It designates a separation of sections within an <article>.",
        },
        {
          optionId: 4,
          optionText:
            "It designates a topic shift within a section at the paragraph level.",
        },
      ],
    },
    {
      questionId: 5,
      question: "When is the <link> tag used?",
      options: [
        {
          optionId: 1,
          optionText:
            "when linking style sheets, JavaScript, and icons for mobile apps",
        },
        {
          optionId: 2,
          optionText:
            "when linking style sheets, favicons, and preloading assets",
        },
        {
          optionId: 3,
          optionText: "when linking style sheets and favicons",
        },
        {
          optionId: 4,
          optionText: "when linking style sheets, external URLs, and favicons",
        },
      ],
    },
  ],
  answers: [
    {
      questionId: 1,
      correctOptionId: 3,
      explanation: "",
    },
    {
      questionId: 2,
      correctOptionId: 3,
      explanation: "",
    },
    {
      questionId: 3,
      correctOptionId: 2,
      explanation: "",
    },
    {
      questionId: 4,
      correctOptionId: 4,
      explanation: "",
    },
    {
      questionId: 5,
      correctOptionId: 3,
      explanation: "",
    },
  ],
};

const cssQuiz = {
  quizName: "CSS",
  quizId: 4,
  quizCardIcon:
    "https://res.cloudinary.com/hntejas/image/upload/v1621609370/quizzer/css-icon.png",
  questions: [
    {
      questionId: 1,
      question:
        "Using an attribute selector, how would you select an <a> element with a 'title' attribute?",
      options: [
        {
          optionId: 1,
          optionText: "a[title]{...}",
        },
        {
          optionId: 2,
          optionText: "a > title {...}",
        },
        {
          optionId: 3,
          optionText: "a.title {...}",
        },
        {
          optionId: 4,
          optionText: "a=title {...}",
        },
      ],
    },
    {
      questionId: 2,
      question:
        "When using position: fixed, what will the element always be positioned relative to?",
      options: [
        {
          optionId: 1,
          optionText: "the closest element with position: relative",
        },
        {
          optionId: 2,
          optionText: "the viewport",
        },
        {
          optionId: 3,
          optionText: "the parent element",
        },
        {
          optionId: 4,
          optionText: "the wrapper element",
        },
      ],
    },
    {
      questionId: 3,
      question:
        "How would you make the first letter of every paragraph on the page red?",
      options: [
        {
          optionId: 1,
          optionText: "p::first-letter { color: red; }",
        },
        {
          optionId: 2,
          optionText: "p:first-letter { color: red; }",
        },
        {
          optionId: 3,
          optionText: "first-letter::p { color: red; }",
        },
        {
          optionId: 4,
          optionText: "first-letter:p { color: red; }",
        },
      ],
    },
    {
      questionId: 4,
      question: "What is the rem unit based on?",
      options: [
        {
          optionId: 1,
          optionText:
            "The rem unit is relative to the font-size of the p element.",
        },
        {
          optionId: 2,
          optionText:
            "You have to set the value for the rem unit by writing a declaration such as rem { font-size: 1 Spx; }",
        },
        {
          optionId: 3,
          optionText:
            "The rem unit is relative to the font-size of the containing (parent) element.",
        },
        {
          optionId: 4,
          optionText:
            "The rem unit is relative to the font-size of the root element of the page.",
        },
      ],
    },
    {
      questionId: 5,
      question: "Which of the following is not a valid color value?",
      options: [
        {
          optionId: 1,
          optionText: "color: #000",
        },
        {
          optionId: 2,
          optionText: "color: rgb(0,0,0)",
        },
        {
          optionId: 3,
          optionText: "color: #000000",
        },
        {
          optionId: 4,
          optionText: "color: 000000",
        },
      ],
    },
  ],
  answers: [
    {
      questionId: 1,
      correctOptionId: 1,
      explanation: "",
    },
    {
      questionId: 2,
      correctOptionId: 2,
      explanation: "",
    },
    {
      questionId: 3,
      correctOptionId: 1,
      explanation: "",
    },
    {
      questionId: 4,
      correctOptionId: 4,
      explanation: "",
    },
    {
      questionId: 5,
      correctOptionId: 4,
      explanation: "",
    },
  ],
};

module.exports = [jsQuiz, reactQuiz, htmlQuiz, cssQuiz];