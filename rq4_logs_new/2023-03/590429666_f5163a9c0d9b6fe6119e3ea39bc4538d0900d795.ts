import Axios from "./adaptor";

export class Accounts {
    static async getListedBanks(token: string) {
        return await Axios.get("/accounts/banks", {
            headers: {
                Authorization: `Bearer ${token}`,
            }
        });
    }
}