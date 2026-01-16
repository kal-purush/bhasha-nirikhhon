const isValidCount = function(count) {
  return count > 0 && isFinite(count);
};

const isValidOption = function(option) {
  return option == "n" || option == "c";
};

const findError = function(args) {
  let {type, option, count} = args;
  let error = "none";
  let isValid = true;
  let errorType;
  if (!isValidOption(option)) {
    errorType = "illegalOption";
    return generateErrorMessage(errorType, args);
  }
  if (!isValidCount(count) && type === "head") {
    errorType = "illegalCount";
    return generateErrorMessage(errorType, args);
  }
  if (!isFinite(count)) {
    errorType = "illegalOffset";
    return generateErrorMessage(errorType, args);
  }
  return {isValid, error};
};

const generateErrorMessage = function(errorType, args) {
  let {type, option, count} = args;
  let countType = {
    n: "line",
    c: "byte"
  };
  let usage = {
    head: "usage: head [-n lines | -c bytes] [file ...]",
    tail: "usage: tail [-F | -f | -r] [-q] [-b # | -c # | -n #] [file ...]"
  };
  let errors = {
    illegalCount:
      type + ": illegal " + countType[option] + " count -- " + count,
    illegalOffset: type + ": illegal offset -- " + count,
    illegalOption: type + ": illegal option -- " + option + "\n" + usage[type]
  };
  let error = errors[errorType];
  let isValid = false;
  return {isValid, error};
};

module.exports = {
  findError,
  generateErrorMessage,
  isValidCount,
  isValidOption
};