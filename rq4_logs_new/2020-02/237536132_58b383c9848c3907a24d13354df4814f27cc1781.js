import numeral from "numeral";

const dollarFilter = function(value) {
  if (!value) {
    return "$ 0";
  }

  return numeral(value).format("($ 0.00a)");
};

const percentFilter = function(value) {
  if (!value) {
    return "0%";
  }

  value = Number(value).toFixed(2);

  return `${value}%`;
};

export { dollarFilter, percentFilter };