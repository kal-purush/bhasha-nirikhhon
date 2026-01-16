import { host, port } from "../../../constants/host";
import { ShopRequest } from "../../request/ShopRequest";
import { ShopResponse } from "../../response/ShopResponse";

const conn = `http://${host}:${port}/api/v1/shop`;

export async function createNewShopInfo(values: ShopRequest) {
  const headers = {
    "Content-type": "application/json; charset=UTF-8",
    Connection: "keep-alive",
    Accept: "*/*",
  };
  var res = await fetch(conn + "/profile", {
    method: "post",
    headers: headers,
    body: JSON.stringify(values),
  });
  if (res.ok) {
    return (await res.json()) as ShopResponse;
  } else {
    return null;
  }
}