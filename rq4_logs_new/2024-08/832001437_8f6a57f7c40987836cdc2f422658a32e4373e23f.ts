type Gender = 'male' | 'female' | 'nonBinary' | 'other';
type Hobby = 'sports' | 'reading' | 'drawing' | 'music' | 'dancing' | 'cooking' | 'video_games' | 'other';
type Superpower =
  | 'flying'
  | 'invisibility'
  | 'super_strength'
  | 'teleportation'
  | 'telekinesis'
  | 'time_travel'
  | 'mind_reading'
  | 'other';

type Animal =
  | 'dog'
  | 'cat'
  | 'fish'
  | 'bird'
  | 'rabbit'
  | 'hamster'
  | 'turtle'
  | 'lizard'
  | 'snake'
  | 'horse'
  | 'guinea pig'
  | 'frog'
  | 'mouse'
  | 'ferret'
  | 'hedgehog'
  | 'chinchilla'
  | 'hermit crab'
  | 'parrot'
  | 'gerbil'
  | 'goat'
  | 'alpaca'
  | 'iguana'
  | 'gecko'
  | 'tarantula'
  | 'scorpion'
  | 'pig'
  | 'cow'
  | 'duck'
  | 'chicken'
  | 'peacock'
  | 'pony'
  | 'lamb'
  | 'raccoon'
  | 'fox'
  | 'owl'
  | 'penguin'
  | 'koala'
  | 'kangaroo'
  | 'sea turtle'
  | 'dolphin'
  | 'whale'
  | 'octopus'
  | 'other';

interface FunNameOptions {
  gender: Gender;
  favoriteAnimal: Animal;
  favoriteSuperpower: Superpower;
  favoriteHobby: Hobby;
}

export const generateFunName = ({ gender, favoriteAnimal, favoriteSuperpower, favoriteHobby }: FunNameOptions) => {
  // Define some prefixes and suffixes for different choices
  const genderPrefix: Record<Gender, string> = {
    male: 'Mighty',
    female: 'Fierce',
    nonBinary: 'Epic',
    other: 'Legendary',
  };

  const hobbyPrefix: Record<Hobby, string> = {
    sports: 'Athletic',
    reading: 'Wise',
    drawing: 'Creative',
    music: 'Melodic',
    dancing: 'Groovy',
    cooking: 'MasterChef',
    video_games: 'Pixel',
    other: 'Amazing',
  };

  const superpowerSuffix: Record<Superpower, string> = {
    flying: 'Soaring',
    invisibility: 'Invisible',
    super_strength: 'Strong',
    teleportation: 'Warping',
    telekinesis: 'Mind-Bending',
    time_travel: 'Timeless',
    mind_reading: 'Mindful',
    other: 'Super',
  };

  // Construct the name
  const prefix = genderPrefix[gender];
  const hobbyPart = hobbyPrefix[favoriteHobby];
  const animalPart = favoriteAnimal.charAt(0).toUpperCase() + favoriteAnimal.slice(1); // Capitalize the first letter
  const suffix = superpowerSuffix[favoriteSuperpower];

  // Combine the parts to make the name
  return `${prefix} ${hobbyPart} ${animalPart} the ${suffix}`;
};