// import { useNavigate } from "react-router-dom";
import { get } from "Utils/api";

export const useUsers = () => {
  // 회원정보
  const getUsers = async () => {
    const res: any = await get("/v1/comment/mypage/1").then((r: any) => {
      console.log(r);
      return { r };
    });
    return { res };
  };
  return { getUsers };
};