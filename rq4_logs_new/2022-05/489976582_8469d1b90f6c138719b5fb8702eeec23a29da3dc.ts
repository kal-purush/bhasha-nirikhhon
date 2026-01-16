function capitalize(str: string){
  let list = str.split('')
  list[0] = list[0].toUpperCase()
  return(list.join(''))
}

function growthRate(gr: string){
  let list = CompostString(gr, '-')
  list = list.map(str => capitalize(str))

  return list.join('-')
}

function CompostString(str: string, char: string = '_'){
  return str.split(char)
}

const Formatting = {
  growthRate,
  capitalize,
}

export default Formatting