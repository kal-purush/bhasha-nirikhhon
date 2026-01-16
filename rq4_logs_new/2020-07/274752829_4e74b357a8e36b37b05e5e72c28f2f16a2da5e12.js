// Given a string, capitalize the letters that occupy even indexes and odd indexes separately, and return as shown below.
// Index 0 will be considered even.
// For example, capitalize("abcdef") = ['AbCdEf', 'aBcDeF'].

const capitalize = (s) => {
  return [
    s
      .split("")
      .map((e, i) => (i % 2 === 0 ? e.toUpperCase() : e.toLowerCase()))
      .join(""),
    s
      .split("")
      .map((e, i) => (i % 2 !== 0 ? e.toUpperCase() : e.toLowerCase()))
      .join(""),
  ];
};