const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

const questions = [
  ["name","What's your name? Nicknames are also acceptable :)"],
  ["hobby", "What's an activity you like doing?"],
  ['music','What do you listen to while doing that?'],
  ['mealtime','Which meal is your favourite (eg: dinner, brunch, etc.)'],
  ['food',"What's your favourite thing to eat for that meal?"],
  ['sport','Which sport is your absolute favourite?'],
  ['talent','What is your superpower? In a few words, tell us what you are amazing at!']
];
let answers = {};


const askQuestions = questions => {
  if (questions.length === 0) {
    rl.close();
    printProfile(answers);
    process.exit(1);
    return;
  }
  const answerID = questions[0][0];
  const question = questions[0][1];
  
  rl.question(question + ': ', (answer) => {
    answers[answerID] = answer;
    
    askQuestions(questions.slice(1))
  });
}

const printProfile = (person) => {
  console.log(`I'm ${person.name} and I enjoy ${person.hobby}. When I need to lose track of time, I pump some ${person.music}.\nBut I will never miss ${person.mealtime}! I love ${person.food} too much!\nThats why I play ${person.sport}. Its fun and healthy.\nIf you get me out to a party, you might see me ${person.talent}`);
}


askQuestions(questions);