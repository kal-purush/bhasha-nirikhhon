import habitInfo from "../../store/habitInfo";
import pillInfo from "../../store/pillInfo";
import { getRequestOptions } from "../utils/getRequestOptions";
import {serverURL} from "../utils/serverURL";


export const deleteChallenge = (challengeToDelete : pillInfo | habitInfo) => {

  const endpoint: string = "supplements/delete/" + challengeToDelete.id;

  // 서버에 요청을 보낸다.
  fetch(serverURL + endpoint, getRequestOptions('PUT', ""))
  .then(response => response.json())
  .then(result => console.log(result.result))
  .catch(error => console.log('editPill Error', error));

}

