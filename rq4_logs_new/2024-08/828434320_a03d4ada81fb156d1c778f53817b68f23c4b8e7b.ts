import { serverURL } from "../utils/serverURL";
import { getRequestOptions } from "../utils/getRequestOptions";

// 서버에서 해당 유저의 활성화 상태의 챌린지를 가져옵니다.
export const loadChallenge = <T>(challengeType:string):Promise<T[]> => {

    let challengeList:T[] = [];

    const endpoint:string = `/${challengeType}/edit`;

  // 서버에 요청을 보낸다.
  return fetch(
        serverURL + endpoint,
      getRequestOptions('GET', "")
  )
  .then(response => response.json())
  .then(result => {challengeList = result.result; console.log(result); return challengeList;})
  .catch(error => {console.log('loadChallenge Error', error); return [] as T[]});

}