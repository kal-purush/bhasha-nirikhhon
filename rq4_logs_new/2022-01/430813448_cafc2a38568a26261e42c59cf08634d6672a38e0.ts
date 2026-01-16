import { getHistoryService } from "backend/services/laravel-api/getHistoryService";
import { postAddInHistory } from "backend/services/laravel-api/postAddInHistory";

export class historyController {
    async getAll(token: string) {

        const service = new getHistoryService();
        return await service.execute(token);
    }

    async addInHistory(token: string, type: number, ad_content: string){
        const service = new postAddInHistory();
        return await service.execute(token, type, ad_content);
    }
}