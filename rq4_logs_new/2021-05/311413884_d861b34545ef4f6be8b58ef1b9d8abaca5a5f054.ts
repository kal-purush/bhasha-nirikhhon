import axios from "axios";
import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import { API_ENDPOINT } from "../../../constants";
import { useAuth } from "../../../context/auth/AuthProvider";

export const useIsMember = () => {
  // https://kentcdodds.com/blog/stop-using-isloading-booleans
  // const { isLoading } = useState(true);
  const router = useRouter();
  const { user } = useAuth({
    redirectTo: "/",
  });
  const {
    query: { serverId },
  } = useRouter();

  useEffect(() => {
    (async () => {
      try {
        if (user.id || serverId) {
          const foo = await axios.get(
            `${API_ENDPOINT}/servers/${serverId}/members/${user.id}`,
            {
              headers: {
                Authorization: `Bearer ${localStorage.getItem("accessToken")}`,
              },
            }
          );
          console.log(foo.data);
        }
      } catch (error) {
        if (
          error.response &&
          error.response.status === 404 &&
          error.response.data.message === "No member found"
        ) {
          router.push("/");
        }
      }
    })();
  }, [serverId, user?.id]);

  return {};
};