export const setItem = (id: string) => {
  console.log(getItems());
  if (getItems().length) {
    const books = getItems();
    books.push(id);
    localStorage.setItem('books', JSON.stringify([...books]));
  } else {
    localStorage.setItem('books', JSON.stringify([id]));
  }
};

export const getItems = (): string[] => {
  if (localStorage.getItem('books') !== null) {
    return [...JSON.parse(localStorage.getItem('books'))];
  } else {
    return [];
  }
};

export const deleteItem = (id: string): void => {
  if (localStorage.getItem('books') !== null) {
    const books = getItems().filter((e) => e !== id);
    localStorage.setItem('books', JSON.stringify(books));
  } else {
    localStorage.setItem('books', JSON.stringify([]));
  }
};