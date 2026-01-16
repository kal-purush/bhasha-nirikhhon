import { AlphabetLetters } from '../types/AppSlice';

const data = {
  themes: {
    countries: [
      'Аргентина',
      'Австралия',
      'Австрия',
      'Бельгия',
      'Бразилия',
      'Канада',
      'Чили',
      'Китай',
      'Колумбия',
      'Чехия',
      'Дания',
      'Финляндия',
      'Франция',
      'Германия',
      'Греция',
      'Гонконг',
      'Венгрия',
      'Индия',
      'Индонезия',
      'Ирландия',
      'Израиль',
      'Италия',
      'Япония',
      'Корея',
      'Малайзия',
      'Мексика',
      'Нидерланды',
      'Норвегия',
      'Филиппины',
      'Польша',
      'Португалия',
      'Россия',
      'Сингапур',
      'Словакия',
      'Испания',
      'Швеция',
      'Швейцария',
      'Тайвань',
      'Таиланд',
      'Турция',
      'Великобритания',
      'США',
      'Вьетнам',
    ],
  },
};

export const getRandomWord = async () => {
  const random = Math.round(Math.random() * (data.themes.countries.length - 1));

  return Promise.resolve({ data: data.themes.countries[random].toLowerCase() });
};

export const bodyParts = [
  'gallows',
  'head',
  'body',
  'leftHand',
  'rightHand',
  'leftLeg',
  'rightLeg',
];

const alphabet: AlphabetLetters[] = [];

for (let i = 'а'.charCodeAt(0); i <= 'я'.charCodeAt(0); i += 1) {
  alphabet.push(String.fromCharCode(i) as AlphabetLetters);
}

export { alphabet };