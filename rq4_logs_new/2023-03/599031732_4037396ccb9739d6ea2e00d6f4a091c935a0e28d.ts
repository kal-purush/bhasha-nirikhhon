import axios from "axios";
import { Dayjs } from "dayjs";
import { SessionResponse } from "../../api/response/SessionResponse";
import { host, port } from "../../constants/host";

export async function getSessionByDate(
  shopId: string,
  accessToken: string,
  date: Dayjs
): Promise<SessionResponse[] | null> {
  try {
    var res = await axios({
      method: "get",
      baseURL: `http://${host}:${port}/api`,
      url: `/v1/session/shop/${shopId}`,
      params: {
        date: date.format("YYYY-MM-DD"),
      },
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        Accept: "*/*",
      },
    });
    return res.data as SessionResponse[];
  } catch (error) {
    return null;
  }
}