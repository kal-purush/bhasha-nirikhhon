import { UserCredential } from "@firebase/auth";
import { logOut } from "../../firebase/firebase";
async function loginGoogle(user: UserCredential) {
  var res = await fetch("http://localhost:8082/api/v1/account/login/google", {
    method: "POST",
    headers: {
      idTokenString: await user.user.getIdToken(),
    },
  });
  if (res.status === 200) {
    let data = await res.json();
    sessionStorage.setItem("accessToken", data.accessToken);
    return true;
  } else {
    logOut();
    sessionStorage.removeItem("accessToken");
    return false;
  }
}

export { loginGoogle };