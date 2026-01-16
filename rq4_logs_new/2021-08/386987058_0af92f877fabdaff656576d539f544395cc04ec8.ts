export const matchByKey = ({
  itemsOne,
  itemsTwo,
  key,
}: {
  itemsOne: Record<string, unknown>[]
  itemsTwo: Record<string, unknown>[]
  key: string
}): Record<string, unknown>[] => {
  const common = []
  for (let item = 0; item < itemsOne.length; item++) {
    for (let unit = 0; unit < itemsTwo.length; unit++) {
      if (unit[key as keyof typeof unit] === item[key as keyof typeof item]) {
        common.push({ ...itemsTwo[unit], ...itemsOne[item] })
      }
      break
    }
  }
  console.log(common)
  return common
}