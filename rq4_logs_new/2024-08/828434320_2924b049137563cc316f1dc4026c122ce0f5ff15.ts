import habitInfo from "../store/habitInfo";
import pillInfo from "../store/pillInfo";

export const getRequestBodyFromChallenge = <T extends pillInfo | habitInfo>(
  challengeToRegister: T
): string => {
  let result: string = "{\n  " + stringifyObjects(challengeToRegister) + "\n}";

  console.log("getRequestBodyFromChallenge: " + result);
  return result;
};

const stringifyObjects = (inputObj: object): string => {
  let result: string = "";
  let arrayOfKeysAndValues = Object.entries(inputObj);
  arrayOfKeysAndValues.map(([key, value], index) => {
    result += `\"${key}\": `;
    result += handleStringByValueTypes(value,index,arrayOfKeysAndValues);
  });

  return result;
};

const stringifyArrays = (inputArr: object): string => {
  let result: string = "";
  let arrayOfKeysAndValues = Object.entries(inputArr);
  arrayOfKeysAndValues.map(([key, value], index) => {
    result += handleStringByValueTypes(value,index,arrayOfKeysAndValues);
  });

  return result;
};

const handleStringByValueTypes = (value: any, index: number, obj: any) => {
    if (isObject(value)) {
        return leftCurl + stringifyObjects(value) + rightCurl(obj, index);
      } else if (isArray(value)) {
        return leftSquare + stringifyArrays(value) + rightSquare(obj, index);
      } else {
        return `\"${value}\"` + `${isLastElement(obj, index) ? "" : ","}\n  `
        console.log(value);
      }
}



const isLastElement = (array: [string, any][], index: number): boolean => {
  return index === array.length - 1;
};

const isObject = (value: any): boolean => {
  return value !== null && typeof value === "object" && !Array.isArray(value);
};

const isArray = (value: any): boolean => {
  return Array.isArray(value);
};

const leftCurl: string = "{\n    ";
// 마지막 원소이면 ,를 붙이지 않는다.
const rightCurl = (array: [string, any][], index: number): string => {
  return `}${isLastElement(array, index) ? "" : ","}\n  `;
};
const leftSquare: string = "[\n    ";
// 마지막 원소이면 ,를 붙이지 않는다.
const rightSquare = (array: [string, any][], index: number): string => {
  return `]${isLastElement(array, index) ? "" : ","}\n  `;
};