export function getInitialsLetters(name: string) {
  const splittedName = name.split(' ')

  if (splittedName.length >= 2) {
    const firstName = splittedName[0]
    const lastName = splittedName[splittedName.length - 1]

    const initialFirstName = firstName[0]
    const initialLastName = lastName[0]

    const iniciais =
      initialFirstName.toUpperCase() + initialLastName.toUpperCase()

    return iniciais
  } else {
    // return <IoPerson color={theme.colors.white} />
    return splittedName[0][0].toUpperCase()
  }
}