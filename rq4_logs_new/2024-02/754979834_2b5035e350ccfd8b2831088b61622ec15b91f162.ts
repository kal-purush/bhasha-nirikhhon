import Axios from "@/config/axios";
import { insertVariables } from "@/config/insert-variables";
import { tryCatch } from "@/config/try-catch";
import { APIRoutes } from "@/constants/routes";

export const fetchFollowLeader = async (postBody: any,) => {
  return tryCatch(
    async () => {
      const res = await Axios.post(APIRoutes.FollowLeader,postBody);
      return res.data;
    }
  );
};

export const fetchUnFollowLeader = async (postBody: any,) => {
  return tryCatch(
    async () => {
      const res = await Axios.post(APIRoutes.UnFollowLeader,postBody);
      return res.data;
    }
  );
};


export const fetchCitizenFollowingList = async (citizenid:string) => {
  return tryCatch(
    async () => {
      const res = await Axios.get(insertVariables(APIRoutes.CitizenFollowingList, { citizenid }));
      return res.data
    }
  );
};

