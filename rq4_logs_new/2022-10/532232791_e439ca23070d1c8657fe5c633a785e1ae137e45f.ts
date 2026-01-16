export const getDays = () => {
  const array = []
  for (let i = 1; i <= 31; i++) {
    if (i >= 10) {
      array.push(i.toString())
    } else array.push(`0${i.toString()}`)
  }
  return array
}

export const getMonths = () => {
  const array = []
  for (let i = 1; i <= 12; i++) {
    if (i >= 10) {
      array.push(i.toString())
    } else array.push(`0${i.toString()}`)
  }
  return array
}

export const getYears = () => {
  const array = []
  for (let i = 1900; i <= 2023; i++) {
    array.push(i.toString())
  }
  return array
}