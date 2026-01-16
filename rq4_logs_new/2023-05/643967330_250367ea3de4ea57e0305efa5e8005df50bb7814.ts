const name = /^([a-zA-Z]{2,}\s[a-zA-Z]{1,}'?-?[a-zA-Z]{2,}\s?([a-zA-Z]{1,})?)/;

const mail = /^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$/g;

const atLestEightChar = /^.{0}$|^.{8,}$/;

const regExp = {
  name,
  mail,
  atLestEightChar,
};

export default regExp;