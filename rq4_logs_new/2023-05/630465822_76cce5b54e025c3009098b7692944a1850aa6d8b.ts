import { StateCreator } from "zustand";
import { allowanceStatusType } from "../../types";
import { staderCommand } from "../staderDaemon"

export interface AllowanceSlice {
    allowanceStatus: allowanceStatusType;
    fetchAllowance: () => void;
}

export const createAllowanceSlice: StateCreator<AllowanceSlice> = (set) => ({
    allowanceStatus: {
        error: "",
        allowance: 0
    },
    fetchAllowance: async () => set({ allowanceStatus: await staderCommand("deposit-sd-allowance") })
})

// staderCommand(`node deposit-sd-allowance`).then((data: any) => {
//     if (data.status === "error") {
//         setFeedback(data.error);
//     } else {
//         const allowance: bigint = BigInt(data.allowance)
//         if (allowance > 0) {
//             setSdApproveButtonDisabled(true);
//             setSdAllowanceOK(true);
//         } else {
//             setSdApproveButtonDisabled(false);
//             setSdAllowanceOK(false);
//         }
//     }
// });