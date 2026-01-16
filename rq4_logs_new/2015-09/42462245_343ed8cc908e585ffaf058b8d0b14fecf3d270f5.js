import _ from "lodash"

const logForEach = (collection) => _.forEach(collection, ::console.log)

export { logForEach }