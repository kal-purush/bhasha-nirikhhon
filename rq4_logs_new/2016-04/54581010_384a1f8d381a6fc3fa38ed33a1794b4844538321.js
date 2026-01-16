export const indices = g => g.reduce((acc, v) => acc.concat(...Object.keys(v)), [])

export const accumArray = edges0 => edges0.reduce((acc, edge) => {
    !acc.hasOwnProperty(edge[0]) ?
      acc[edge[0]] = [].concat([edge[1]]) :
      acc[edge[0]] = acc[edge[0]].concat([edge[1]])

      if (!acc[edge[0]] || !acc[edge[1]]) {
        acc[edge[1]] = []
        return acc
      }

    return acc
  }, {})