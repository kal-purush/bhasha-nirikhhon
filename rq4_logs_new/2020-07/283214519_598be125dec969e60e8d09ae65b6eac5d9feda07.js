const name = "Katie Braun";
const age = 28;
const birthday = "February 20";
const detroitGC = true;
const lifeEvents = [
  "In 1992 I was born in Lake Forest, IL",
  "In 2010 I went to Ringling College of Art & Design",
  "In 2014 I adopted my cat Sophia",
  "In 2020 I started to learn how to code",
];

if (detroitGC) {
  console.log(
    `My name is ${name} and I am a student at Grand Circus in Detroit, Michigan. I am currently ${age} years old and my birthday is on ${birthday}.`
  );
} else {
  console.log(
    `My name is ${name} and I am a student at Grand Circus in Grand Rapids, Michigan. I am currently ${age} years old and my birthday is on ${birthday}.`
  );
}

for (let i = 0; i < 4; i++) {
  console.log(lifeEvents[i]);
}

let counter = 0;

while (true) {
  let randomNumber = Math.floor(Math.random() * 10 + 1);

  if (randomNumber !== 5) {
    counter++;
    console.log(`${randomNumber} !== 5`);
  } else {
    counter++;
    console.log(
      `5 === 5. It took ${counter} iterations to random generate the number 5.`
    );
    break;
  }
}