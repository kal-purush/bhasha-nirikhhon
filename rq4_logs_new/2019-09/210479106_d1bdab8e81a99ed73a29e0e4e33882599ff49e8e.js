const title = 'littleRed'

console.log(title)

const story = {
  color: 'Red',
  nouns: ['apples', 'ears', 'eyes', 'hands'],
  adjective: 'strong',
  exclamation: 'Hey',
  silly: 'hullabaloo',
  verbs: ['went', 'hear', 'see']
}

console.log(`One day, Little ${story.color} Riding Hood was going through the forest carrying a basket of ${story.nouns[0]} for her grandmother. Suddenly, she met a big ${story.adjective} wolf.

"${story.exclamation}" said the wolf. "Where are you going little ${story.silly}?"

"I'm going to my grandmother's house," she said. Then the wolf ${story.verbs[0]} away.

When Miss Riding Hood got to her grandmother's house, the wolf was in bed dressed like her grandmother.

"My, Grandmother," she said. "What big ${story.nouns[1]} you have."

"The better to ${story.verbs[1]} you with," said the wolf.

"And, Grandmother," she said, "what big ${story.nouns[2]} you have."

The wolf said, "The better to ${story.verbs[2]} you with."

And then she said, "What big ${story.nouns[3]} you have, Grandmother." But the wolf said nothing. He had just died of indigestion from eating Grandmother.`)

story.color = 'Blue'
story.nouns = ['pears', 'legs', 'lips', 'arms']
story.exclamation = 'Come'
story.verbs = ['left', 'catch', 'eat']

console.log(`One day, Little ${story.color} Riding Hood was going through the forest carrying a basket of ${story.nouns[0]} for her grandmother. Suddenly, she met a big ${story.adjective} wolf.

"${story.exclamation}" said the wolf. "Where are you going little ${story.silly}?"

"I'm going to my grandmother's house," she said. Then the wolf ${story.verbs[0]} away.

When Miss Riding Hood got to her grandmother's house, the wolf was in bed dressed like her grandmother.

"My, Grandmother," she said. "What big ${story.nouns[1]} you have."

"The better to ${story.verbs[1]} you with," said the wolf.

"And, Grandmother," she said, "what big ${story.nouns[2]} you have."

The wolf said, "The better to ${story.verbs[2]} you with."

And then she said, "What big ${story.nouns[3]} you have, Grandmother." But the wolf said nothing. He had just died of indigestion from eating Grandmother.`)

const name = 'snowWhite'

console.log(name)

const content = {
  plural: ['stories', 'Dwarfs'],
  number: 'Seven',
  adjectives: ['pretty', 'cute', 'warm'],
  noun: ['castle', 'forest', 'city', 'kiss', 'apple'],
  colors: 'White',
  body: 'lips',
  adverb: 'happily'
}

console.log(`One of the most popular fairy ${content.plural[0]} of all time is Snow White and the ${content.number} ${content.plural[1]}.

Snow White is a princess whose ${content.adjectives[0]} beauty threatens her stepmother, the queen. Snow White is forced to flee from the ${content.noun[0]} in which she lives and hide in a nearby ${content.noun[1]}.

Once there, she is discovered by ${content.adjectives[1]} animals who guide her to the ${content.adjectives[2]} cottage of the seven dwarfs. The dwarfs take care of her until the prince, who has traveled the four corners of the ${content.noun[2]} in search of Snow ${content.colors}, arrives and gives her a magical ${content.noun[3]} on her ${content.body} which miraculously bring her back to life after she eats a poisonous ${content.noun[4]}. Snow White and the prince live ${content.adverb} ever after.`)

content.number = 'Eight'
content.adjectives = ['elegant', 'lovely', 'beautiful']
content.noun = ['palace', 'wood', 'country', 'hug', 'pear']
content.colors = 'Red'

console.log(content)