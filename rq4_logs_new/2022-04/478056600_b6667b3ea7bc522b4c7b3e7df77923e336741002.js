const isEqual = require('./isEqual.js').isEqual;

const contains = function (array, element) {
  if (!Array.isArray(element)) {
    return array.includes(element);
  }

  for (let index = 0; index < array.length; index++) {
    if (isEqual(element, array[index])) {
      return true;
    }
  }
  return false;
};

const uniqueElements = function (array) {
  const unique = [];
  for (let index = 0; index < array.length; index++) {
    if (!contains(unique, array[index])) {
      unique.push(array[index]);
    }
  }
  return unique;
};

const elementGroup = function (element, array) {
  const group = [];
  for (let index = 0; index < array.length; index++) {
    if (isEqual(element, array[index])) {
      group.push(array[index]);
    }
  }
  return group;
};

const groupSameElements = function (array) {
  const unique = uniqueElements(array);
  const groupedElements = [];
  for (let index = 0; index < unique.length; index++) {
    groupedElements.push(elementGroup(unique[index], array));
  }
  return groupedElements;
};

exports.groupSameElements = groupSameElements;
exports.uniqueElements = uniqueElements;