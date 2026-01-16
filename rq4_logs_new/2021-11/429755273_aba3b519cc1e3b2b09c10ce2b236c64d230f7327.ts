export type CatType = {
  id: number;
  name: string;
  breed: string;
  age: string;
  description: string;
  img: string;
};

export const Cats: Array<CatType> = [
  {
    id: 1,
    name: 'Габи',
    breed: 'Чеширская кошка',
    age: '2 года',
    description: 'Порода домашних кошек, выведенная в Великобритании в конце XIX века.',
    img: 'https://upload.wikimedia.org/wikipedia/commons/thumb/f/f0/Australian_Mist.jpg/236px-Australian_Mist.jpg',
  },
  {
    id: 2,
    name: 'Пушок',
    breed: 'Чеширская кошка',
    age: '3 года',
    description: 'Описание: Хорошая кошка',
    img: 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/Kamee01.jpg/396px-Kamee01.jpg',
  },
  {
    id: 3,
    name: 'Снежок',
    breed: 'Чеширская кошка',
    age: '2 года',
    description: 'Хорошая кошка',
    img: 'https://upload.wikimedia.org/wikipedia/commons/thumb/f/f0/Australian_Mist.jpg/236px-Australian_Mist.jpg',
  },
  // {
  //   id: 4,
  //   name: 'Ченя',
  //   breed: 'Чеширская кошка',
  //   age: '2 года',
  //   description: 'Хорошая кошка',
  //   img: 'https://upload.wikimedia.org/wikipedia/commons/thumb/f/f0/Australian_Mist.jpg/236px-Australian_Mist.jpg',
  // },
  // {
  //   id: 5,
  //   name: 'Вася',
  //   breed: 'Чеширская кошка',
  //   age: '2 года',
  //   description: 'Хорошая кошка',
  //   img: 'https://upload.wikimedia.org/wikipedia/commons/thumb/f/f0/Australian_Mist.jpg/236px-Australian_Mist.jpg',
  // },
  // {
  //   id: 6,
  //   name: 'Рауль',
  //   breed: 'Чеширская кошка',
  //   age: '2 года',
  //   description: 'Хорошая кошка',
  //   img: 'https://upload.wikimedia.org/wikipedia/commons/thumb/f/f0/Australian_Mist.jpg/236px-Australian_Mist.jpg',
  // },
];

// export type { CatType };
// export { Cats };