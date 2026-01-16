//using `` in consolelog and || for or statements

let tool = 'marker';

// Use short circuit evaluation to assign  writingUtensil variable below:
let writingUtensil = tool || 'pen'

console.log(`The ${writingUtensil} is mightier than the sword.`);




//different exercise with understood 'truthy', no need to write it
let wordCount = 1;

if (wordCount) {
  console.log("Great! You've started your work!");
} else {
  console.log('Better get to work!');
}


let favoritePhrase = '';

if (favoritePhrase) {
  console.log("This string doesn't seem to be empty.");
} else {
  console.log('This string is definitely empty.');
}


//using ! and ||   if and else
let mood = 'sleepy';
let tirednessLevel = 6;

if (mood !== 'sleepy' || tirednessLevel !> 8) {
  console.log('time to sleep')
} else {
  console.log('not bed time yet')
}