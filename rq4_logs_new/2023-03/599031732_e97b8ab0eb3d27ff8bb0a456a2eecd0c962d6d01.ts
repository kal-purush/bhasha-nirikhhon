import { useEffect, useState } from "react";
import { useAuthState } from "react-firebase-hooks/auth";
import { auth } from "../firebase/firebase";

function useUserLogin() {
  const [user] = useAuthState(auth);
  const [isLoggedIn, setIsLoggedIn] = useState(false);
  useEffect(() => {
    function handleLoggedIn(status: boolean) {
      setIsLoggedIn(status);
    }
    var accessToken = sessionStorage.getItem("accessToken");
    // if () {
    //   setIsLoggedIn(true);
    // } else {
    //   setIsLoggedIn(false);
    // }
    setIsLoggedIn(user != null && accessToken != null);
    // window.addEventListener("storage", () => {
    //   console.log("hook triggered");
    //   var accessToken = sessionStorage.getItem("accessToken");
    //   if (user) {
    //     console.log("accesstoken found. Signing in");
    //     handleLoggedIn(
    //       accessToken != null && accessToken === "accessTokenVerified"
    //     );
    //   }
    // });
    // localStorage.removeItem("notify");
    // return () => {
    //   window.removeEventListener("storage", () => {
    //     console.log("hook triggered");
    //     var accessToken = sessionStorage.getItem("accessToken");
    //     if (user) {
    //       console.log("accesstoken found. Signing in");
    //       handleLoggedIn(
    //         accessToken != null && accessToken === "accessTokenVerified"
    //       );
    //     }
    //     localStorage.removeItem("notify");
    //   });
    // };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  return isLoggedIn;
}

export default useUserLogin;