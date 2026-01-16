import axios from "axios";
import { Container } from "../Container";
import Cookies from "js-cookie";

export async function deleteContainer(containerId: number): Promise<boolean> {
  const URL = `http://34.143.216.186:28762/api/containers`;
  console.log(containerId)

  try {
    axios.delete(URL, {
      params: { cid: containerId },
      headers: {
        authorization: Cookies.get("authorization"),
      },
    });
    return true;
  } catch (err: any) {
    if (err.response) {
      // The client was given an error response (5xx, 4xx)
      console.log(err.response.data);
      console.log(err.response.status);
      console.log(err.response.headers);
    } else if (err.request) {
      // The client never received a response, and the request was never left
      console.log(err.request);
    } else {
      // Anything else
      console.log("Error", err.message);
    }
  }
  return false;
}