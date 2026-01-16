export function criaUserName(nomeCompleto: string): string {
  const arrayNomeCompleto = nomeCompleto.toLocaleLowerCase().trim().split(" ");

  if (arrayNomeCompleto.length > 2) {
    const userName = `${arrayNomeCompleto[0].slice(0, 3)}${
      arrayNomeCompleto[1][0]
    }_${arrayNomeCompleto[arrayNomeCompleto.length - 1]}`;
    return userName;
  }
  const userName = `${arrayNomeCompleto[0].slice(0, 3)}${
    arrayNomeCompleto[0][arrayNomeCompleto[0].length - 1]
  }_${arrayNomeCompleto[arrayNomeCompleto.length - 1]}`;
  return userName;
}