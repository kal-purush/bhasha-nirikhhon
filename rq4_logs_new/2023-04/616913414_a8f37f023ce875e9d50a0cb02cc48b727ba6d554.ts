import SimulationService from "./service";
import { UseQueryResult, useQuery } from "@tanstack/react-query";
import { useRecoilState } from "recoil";
import {PopupModalSubmissionOpen} from "./store"
import { ReturnTypesPopupSubmissionStatus, TSimulationResponse } from "./type";

export const useGetAllSimulation = (): UseQueryResult<TSimulationResponse, unknown> =>
    useQuery({
        queryKey: ["get-all-simulation"],
        queryFn: async()=>await SimulationService.GetAllSimulation(),
    });

export const usePopupSubmissionStatus = (): ReturnTypesPopupSubmissionStatus => {
  const [get, set] = useRecoilState(PopupModalSubmissionOpen);
  return {
    setPopupStatus: (val: boolean) => set(val),
    getPopupStatus: get,
  };
};
