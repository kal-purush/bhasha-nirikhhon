export default function (line, pictures) {
  let newFlyPicture = null; // будующий flyPicture
  const newLine = [...line]; // Почемуто быстрее работает если копирывать массив

  for (let i = 0; i < newLine.length; i++) {
    for (let j = 0; j < newLine[i].length; j++) {
      if (newLine[i][j].id === pictures.id) {
        // Нашли элемент
        newFlyPicture = [newLine[i][j]]; // Сохраняем найденный элемент

        newLine[i].splice(j, 1); // Удаляем элемент из подмассива
        return [newFlyPicture, newLine];
      }
    }
  }
}