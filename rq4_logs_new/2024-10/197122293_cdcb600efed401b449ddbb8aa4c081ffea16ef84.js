// const example = '../assets/Products/example.jpeg';
const { GENRES, METHODS, PROTEIN, SECTIONS, TYPES, CATEGORIES } = require('./constants');
const { SEMI_SWEET_CHOCOLATE_CHIPS, PEANUT_BUTTER, CHEX_CEREAL, POWDERED_SUGAR } = require('./ingredients');

module.exports = {
    cardName: 'Muddy Buddies',
    name: 'Muddy Buddies',
    img: '',
    available: true,
    category: [CATEGORIES.DESSERT],
    genre: [GENRES.AMERICAN],
    method: [METHODS.MIX],
    protein: [PROTEIN.PEANUT],
    type: [TYPES.DESSERT],
    yields: ' servings',
    prepTime: 'm',
    cookTime: 'm',
    websites: [
        { label: 'Muddy Buddies', link: 'https://joyfoodsunshine.com/puppy-chow-recipe/' }
    ],
    separated: true,
    ingredients: [
        { ...SEMI_SWEET_CHOCOLATE_CHIPS, amount: '1 c', additionalDetails: '', section: SECTIONS.MAIN },
        { ...PEANUT_BUTTER, amount: '1 c', additionalDetails: '', section: SECTIONS.MAIN },
        { ...CHEX_CEREAL, amount: '6 c', additionalDetails: '', section: SECTIONS.MAIN },
        { ...POWDERED_SUGAR, amount: '1 1/2 c', additionalDetails: '', section: SECTIONS.MAIN },
    ],
    appliances: [
        { name: 'fridge' },
        { name: 'microwave or stove' },
    ],
    supplies: [
        { name: 'mixing bowl' },
        { name: 'microwavable bowl or saucepan' },
    ],
    directions: [
        { step: 'Either over the stove or in a microwavable bowl, melt the peanut butter and chocolate chips.', type: SECTIONS.MAIN },
        { step: 'In a mixing bowl, add 3 cups of cereal.', type: SECTIONS.MAIN },
        { step: 'Pour 1 cup of the chocolate mixture over the cereal.', type: SECTIONS.MAIN },
        { step: 'Mix until evenly coated.', type: SECTIONS.MAIN },
        { step: 'Repeat this process with adding 3 more cups of cereal and the remaining chocolate mixture.', type: SECTIONS.MAIN },
        { step: 'Mix until evenly coated.', type: SECTIONS.MAIN },
        { step: 'If there are pools of the chocolate mixture, add 1/4 cup of cereal at a time until all the chocolate mixture is used.', type: SECTIONS.MAIN },
        { step: 'Place this in the fridge for maybe 15 minutes. You do not want it to harden because it makes the powdered sugar more difficult to stick.', type: SECTIONS.MAIN },
        { step: 'Let the mixture come to room temperature.', type: SECTIONS.MAIN },
        { step: 'Add 1 cup of powdered sugar and mix.', type: SECTIONS.MAIN },
        { step: 'Chill in the fridge again for 15 minutes.', type: SECTIONS.MAIN },
        { step: 'Add 1/4 cup of powdered sugar and mix. Repeat with the remaining powdered sugar.', type: SECTIONS.MAIN },
        { step: 'Enjoy these sweet goodies while watching a movie or reading a book.', type: SECTIONS.SERVE },
        { step: 'Store at room temperature in an airtight container.', type: SECTIONS.STORE },
    ]
};