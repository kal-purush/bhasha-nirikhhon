
export function collapse(source: any): any {
  let result = {}

  Object.keys(source).forEach(key => {
    let data = source[key]

    if (data)[
      result[key] = data
    ]
  })

  return result
}

export default collapse