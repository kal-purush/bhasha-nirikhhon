const library = [];

const addBook = (name, autor, year, genre, pages) => (
  library.push({ name, autor, year, genre, pages })
);

const removeBook = (nameBook) => {
  const filtered = library.filter(({ name }) => name !== nameBook);

  library.length = 0;
  library.push(...filtered);

  return;
};

const findBooksByAuthor = (name) => (
  library.filter(({ autor }) => autor === name)
);

const filterBooksByGenre = (genreBook) => (
  library.filter(({ genre }) => genre === genreBook)
);

const genreReport = () => (
  library.reduce((acc, { genre }) => (
    { ...acc, [genre]: (acc[genre] ?? 0) + 1 }
  ), {})
);

const averagePagesReport = () => {
  const countBook = library.length || 1;
  const sumPages = library.reduce((acc, { pages }) => acc += pages, 0);

  return Math.round(sumPages / countBook);
};


const sortedNewToOld = () => (
  library.sort(({ year: a }, { year: b }) => b - a)
);


// addBook("Хоббит", "Дж. Р. Р. Толкиен", 1937, "Фэнтези", 310);
// addBook("Гарри Поттер и философский камень", "Дж. К. Роулинг", 1997, "Фэнтези", 223);
// addBook("1984", "Джордж Оруэлл", 1949, "Антиутопия", 328);
// console.log(library);

// // Поиск книг по автору
// console.log(findBooksByAuthor("Дж. Р. Р. Толкиен"));
// // Фильтрация книг по жанру
// console.log(filterBooksByGenre("Фэнтези"));
// // Генерация отчета по количеству книг каждого жанра
// console.log(genreReport());
// // Генерация отчета по среднему количеству страниц
// console.log(`Среднее количество страниц: ${averagePagesReport()}`);
// // Удаление книги и вывод обновленной библиотеки
// removeBook("1984");
// console.log(library);
// // Сортировка
// console.log(sortedNewToOld());