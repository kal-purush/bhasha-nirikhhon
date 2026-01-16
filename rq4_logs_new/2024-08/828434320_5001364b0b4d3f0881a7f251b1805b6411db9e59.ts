import habitInfo from "../store/habitInfo";
import pillInfo from "../store/pillInfo";
import { tokenAsString } from "./tokenAsString";

let header = new Headers();
header.append("Jwt", tokenAsString);


// var requestOptions = {
//   method: 'POST',
//   headers: header,
//   body: raw,
//   redirect: 'follow'
// };

export const registerPill = (pillToRegister : pillInfo) => {

  let requestBody = getRequestBodyFromChallenge<pillInfo>(pillToRegister);

  // // 서버에 요청을 보낸다.
  // fetch("localhost:9000/supplements/register", requestOptions)
  // .then(response => response.text())
  // .then(result => console.log(result))
  // .catch(error => console.log('registerPill Error', error));
}

const getRequestBodyFromChallenge = <T extends pillInfo | habitInfo>(challengeToRegister: T):string => {
  
  let result:string = "{\n  " + stringifyObjects(challengeToRegister) + "\n}";

  console.log("getRequestBodyFromChallenge: "+result);
  return result;

}

const isObject = (value: any): boolean => {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

const isArray = (value: any): boolean => {
  return Array.isArray(value);
}

const stringifyObjects = (inputObj: object):string => {
  let result:string = "";
  let arrayOfKeysAndValues = Object.entries(inputObj);
  arrayOfKeysAndValues.map(([key, value], index) => {
    result += `\"${key}\": `;
    if(isObject(value)){
      result += "{\n    ";
      result += stringifyObjects(value);
      result += `}${isLastElement(arrayOfKeysAndValues,index) ? "" : ","}\n  `;
    } else if(isArray(value)){
      result += "[\n    ";
      result += stringifyArrays(value);
      result += "]\n";
    } else {
      result += `\"${value}\"${isLastElement(arrayOfKeysAndValues,index) ? "" : ","}\n  `
      console.log(value);
    }
  });

  return result;
}


const stringifyArrays = (inputArr: object):string => {
  let result:string = "";
  let arrayOfKeysAndValues = Object.entries(inputArr);
  arrayOfKeysAndValues.map(([key, value], index) => {
    if(isObject(value)){
      result += "{\n    ";
      result += stringifyObjects(value);
      result += `}${isLastElement(arrayOfKeysAndValues,index) ? "" : ","}\n  `;
    } else if(isArray(value)){
      result += "[\n    ";
      result += stringifyArrays(value);
      result += "]\n";
    } else {
      result += `\"${value}\"${isLastElement(arrayOfKeysAndValues,index) ? "" : ","}\n  `
      console.log(value);
    }
  });

  return result;
}

const isLastElement = (array: [string, any][], index: number):boolean => {
  return index === (array.length - 1);
}