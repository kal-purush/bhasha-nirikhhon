export type Ingredient = {
  id: number;
  name: string;
  pricePerUnit: number;
  unit?: string;
  img?: string;
};

export type Member = {
  name: string;
  email: `${string}@${string}`;
  id: number;
};

// type PropType<T, P extends keyof T> = T[P];
// type IdType = PropType<Member, 'id'>;

export type WeekThing = {
  members: Map<number, Member>;
  ingredients: Map<number, Ingredient>;
};

const allIngredients: Ingredient[] = [
  {
    name: 'Bread',
    id: 1,
    pricePerUnit: 1.99,
    unit: 'unit',
    img: 'https://static.countdown.co.nz/assets/product-images/zoom/9415142008554.jpg',
  },
  {
    name: 'Cheese',
    id: 2,
    pricePerUnit: 15.7,
    img: 'https://static.countdown.co.nz/assets/product-images/zoom/9421018080036.jpg',
  },
  {
    name: 'Lettuce',
    id: 3,
    pricePerUnit: 2.99,
    unit: 'unit',
    img: 'https://static.countdown.co.nz/assets/product-images/zoom/9421026690722.jpg',
  },
  {
    name: 'Tomato',
    id: 4,
    pricePerUnit: 1.99,
    unit: 'kg',
    img: 'https://static.countdown.co.nz/assets/product-images/zoom/265691000009.jpg',
  },
  {
    name: 'Chicken',
    id: 5,
    pricePerUnit: 9.99,
    img: 'https://static.countdown.co.nz/assets/product-images/zoom/9414735954537.jpg',
  },
  {
    name: 'Ham',
    id: 6,
    pricePerUnit: 1.99,
    unit: '100g',
    img: 'https://static.countdown.co.nz/assets/product-images/zoom/272418000006.jpg',
  },
  {
    name: 'Salami',
    id: 7,
    pricePerUnit: 3.99,
    img: 'https://static.countdown.co.nz/assets/product-images/large/270095000005.jpg',
  },
  {
    id: 8,
    name: 'BBQ Sauce',
    pricePerUnit: 3,
    img: 'https://static.countdown.co.nz/assets/product-images/zoom/9419741000681.jpg',
  },
  {
    id: 9,
    name: 'Gherkins',
    pricePerUnit: 3,
    img: 'https://static.countdown.co.nz/assets/product-images/zoom/9416107513489.jpg',
  },
  {
    id: 10,
    name: 'Sriracha',
    pricePerUnit: 6.99,
    img: 'https://static.countdown.co.nz/assets/product-images/zoom/24463061095.jpg',
  },
];

const allMembers: Member[] = [
  {
    id: 1,
    name: 'Kelvin',
    email: 'kelvin.lau@trademe.co.nz',
  },
  {
    id: 2,
    name: 'Marcus',
    email: 'marcus.ball@trademe.co.nz',
  },
  {
    id: 3,
    name: 'Max',
    email: 'max.mclaren@trademe.co.nz',
  },
  {
    id: 4,
    name: 'Bo',
    email: 'bo.han@trademe.co.nz',
  },
  {
    id: 5,
    name: 'Dom',
    email: 'dominic.kim@trademe.co.nz',
  },
];

const weeklyThing: { [week: number]: WeekThing } = {
  21: {
    members: new Map(),
    ingredients: new Map(),
  },
};

export const getAllIngredients = () => allIngredients;

export const getAllMembers = () => allMembers;
export const getMemberById = (id: number) => allMembers.find((m) => m.id === id);

export const getCurrentWeek = () => 21;
export const getWeekThing = (week: number) => {
  const thing = weeklyThing[week];
  if (thing) {
    return thing;
  }

  const newThing: WeekThing = {
    members: new Map(),
    ingredients: new Map(),
  };
  weeklyThing[week] = newThing;
  return newThing;
};

export const enrolMember = (week: number, memberId: number) => {
  if (week < getCurrentWeek()) {
    throw new Error(`Can't enrol in past week`);
  }

  const weekThing = getWeekThing(week);
  weekThing.members.set(
    memberId,
    allMembers.find((m) => m.id === memberId)
  );
};

export const withdrawMember = (week: number, memberId: number) => {
  if (week < getCurrentWeek()) {
    throw new Error(`Can't withdraw from past week`);
  }

  const weekThing = getWeekThing(week);
  weekThing.members.delete(memberId);
};

export const addIngredient = (week: number, ingredientId: number) => {
  if (week <= getCurrentWeek()) {
    throw new Error(`Can't change this week or past weeks`);
  }

  const weekThing = getWeekThing(week);
  weekThing.ingredients.set(
    ingredientId,
    allIngredients.find((i) => i.id === ingredientId)
  );
};

export const removeIngredient = (week: number, ingredientId: number) => {
  if (week <= getCurrentWeek()) {
    throw new Error(`Can't change this week or past weeks`);
  }

  const weekThing = getWeekThing(week);
  weekThing.ingredients.delete(ingredientId);
};