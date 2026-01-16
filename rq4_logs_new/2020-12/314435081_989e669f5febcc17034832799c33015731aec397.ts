export const numberStringToPhoneNumber = (text: string) => {
  if (!text) {
    return "";
  }

  if (text.length < 10 || text.length > 11) {
    return text;
  }

  let formattedPhone = text;

  let numberWithoutDDD = formattedPhone.substr(2).padStart(9, "9");

  formattedPhone = `(${formattedPhone.substr(0, 2)}) ${numberWithoutDDD.substr(
    0,
    5,
  )}-${numberWithoutDDD.substr(5)}`;

  return formattedPhone;
};

export const phoneNumberToNumberString = (phoneNumber: string) => {
  return phoneNumber
    .split("-")
    .join("")
    .split(" ")
    .join("")
    .replace("(", "")
    .replace(")", "");
};