console.log("wysiwyg.js is loaded");

////////////////////////// Instructions //////////////////////////
// // 1. Create an array of objects that represents famous people (see structure below).
var bluthMember = [
  {
    title: "Bluth Company President (de facto)",
    name: "Nichael Bluth",
    bio: "Michael Bluth is the hard working (and therefore self-righteous) member of the Bluth family. Never getting the natural favor that his older brother received, Michael worked hard to gain his father's approval. His father, meanwhile, decided to never show favor as a motivation technique. Michael married his college sweetheart Tracey and they became happy parents to George Michael Bluth.",
    image: "images/NichaelBluth.jpg",
    lifespan: {
      birth: 1970,
      death: "n/a"
    }
  },
    {
    title: "Magician",
    name: "George Oscar Bluth",
    bio: "G.O.B. (short for George Oscar Bluth) is the oldest of the five Bluth children. Named after his father, G.O.B. inherited none of George's intelligence or motivation. His greed led him to the life of a magician, among other quick-income professions such as male stripping. After receiving media attention revealing how an illusion worked, G.O.B. was blacklisted from magic and struggled to make his way back into the inner circle. ",
    image: "GOB.jpg",
    lifespan: {
      birth: 1969,
      death: "n/a"
    }
  },
    {
    title: "Army (Excused From Service)",
    name: "Buster Bluth",
    bio: "Buster Bluth, real name Byron, is the baby of the Bluth family, despite being in his thirties. He is extremely dependent on his mother Lucille, perhaps because he spent 11 months in her womb. Although raised to believe George was his father, he eventually learned that Oscar Bluth, George's identical twin brother, was having an affair with Lucille and is probably his biological father. Buster was a scholar until the family went broke, and served in Army for a moment until his left hand was bitten off by a loose seal. ",
    image: "images/BusterBluth.jpg",
    lifespan: {
      birth: 1973,
      death: "TBD"
    }
  },
    {
    title: "Bluth Company ",
    name: "Lindsay Bluth Fünke",
    bio: "Lindsay Bluth Fünke, né Nellie, was adopted by the Bluth family at age three and told she was a fraternal twin to Michael. Appreciated only for her looks by her parents, Lindsay began acting out in rebellion, including moving to Boston, marrying Tobias Fünke and hosting charity dinners to spend her time. Lindsay and Tobias had to turn to a doctor to get pregnant but eventually gave birth to Maeby Fünke.",
    image: "images/LindsayBluth.jpg",
    lifespan: {
      birth: 1971,
      death: "TBD"
    }
  },
    {
    title: "Bluth Company Founder",
    name: "George Bluth",
    bio: "George Bluth is the patriarch of the Bluth family. After founding the Bluth Company in 1953, George stole his soon-to-be-wife Lucille from his twin brother Oscar and forced him out of the real estate business. In 2003 he was arrested by the SEC for stealing money from his company but was ultimately pardoned by the CIA.",
    image: "images/GeorgeBluth.jpg",
    lifespan: {
      birth: 1944,
      death: "TBD"
    }
  },
    {
    title: "Bluth Family Matriarch",
    name: "Lucille Bluth",
    bio: "Lucille Bluth is the matriarch, and to a degree the puppet master of the Bluth family (no, not including Franklin). Using insults as a form of parenting, Lucille longs for others to be jealous of her and is a functional alcoholic. She is manipulative and cold, but at times displays warmness and protection for her children. She was arrested after George's pardon for masterminding George's fraud.",
    image: "images/LucilleBluth.jpg",
    lifespan: {
      birth: 1949,
      death: "TBD"
    }
  }
];
// // 2. Create a text input in your DOM.
var updateBioTextEl = document.getElementById("updateBioText"), //basically the "TEXT INPUT" AREA
    counter = 0;




// // 3. Beneath that, create a container, block element in your DOM.
    // //  See index.html
// // 4. Create a DOM element for each of the objects inside the container. Style your person elements however you like.
// // 5. For every even numbered element, have a light yellow background.
// // 6. For every odd numbered element, have a light blue background.
// // 7. Each element's DOM structure should be as shown below.
// // 8. When you click on one of the person elements, a dotted border should appear around it.
// // 9. When you click on one of the person elements, the text input should immediately gain focus so that you can start typing.
// // 10. When there is a highlighted person element, and you begin typing in the input box, the person's biography should be immediately bound to what you are typing, letter by letter.
// // 11. When you press the enter/return key when typing in the input field, then the content of the input field should immediately be blank.