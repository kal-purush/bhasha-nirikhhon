
const add = (event) => {
  const list = []
  list.push({ description: event.target.value, completed: false, index: 0 });
  return list.length;
};


module.exports = { add };