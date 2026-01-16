import { UseQueryResult, useQuery } from "@tanstack/react-query";
import { useRecoilState } from "recoil";
import { PopupProfilLeaderborad, popupGetUser } from "./store";
import { ReturnTypesPopupProfil, ReturnTypesPopupGetUser, TLeaderboardItem, TLeaderboardResponse } from "./type";
import LeaderboardService from "./service";

export const usePopupProfilLeaderboard = (): ReturnTypesPopupProfil => {
  const [get, set] = useRecoilState(PopupProfilLeaderborad);
  return {
    getPopupLeaderboardStatus: get,
    setPopupLeaderboardStatus: (val: boolean) => set(val),
  };
};

export const usePopupGetUser = (): ReturnTypesPopupGetUser => {
  const [get, set] = useRecoilState(popupGetUser);
  return {
    setPopupUser: (val: TLeaderboardItem) => set(val),
    getPopupUser: get,
  };
};

export const useGetAllLeaderboard = (): UseQueryResult<TLeaderboardResponse, unknown> =>
  useQuery<TLeaderboardResponse>({
    queryKey: ["get-all-leaderboard"],
    queryFn: async () => await LeaderboardService.GetAllLeaderboard(),
  });
