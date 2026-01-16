import Axios from "@/config/axios";
import { insertVariables } from "@/config/insert-variables";
import { tryCatch } from "@/config/try-catch";
import { APIRoutes } from "@/constants/routes";

export const DeactiveAccount = async (citizenid: string) => {
    return tryCatch(
        async () => {
            const res = await Axios.get(insertVariables(APIRoutes.DeactiveAccount, { citizenid }));
            return res.data;
        }
    );
};

export const CloseAccount = async (citizenid: string) => {
    return tryCatch(
        async () => {
            const res = await Axios.get(insertVariables(APIRoutes.CloseAccount, { citizenid }));
            return res.data;
        }
    );
};