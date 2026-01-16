import axios from "../api/api";
import useAuth from "./useAuth";

const useLogout = () => {
  const { setAuth } = useAuth();

  const logout = async () => {
    setAuth({
      user: {
        id: "",
        username: "",
        email: "",
        password: "",
        role: "",
        isEmailVerified: false,
      },
      accessToken: { token: "", expires: new Date(0) },
    });
    try {
      const response = await axios("/logout", {
        withCredentials: true,
      });
      return response;
    } catch (err) {
      console.error(err);
    }
  };

  return logout;
};

export default useLogout;