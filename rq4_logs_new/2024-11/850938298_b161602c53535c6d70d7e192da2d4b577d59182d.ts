
import { HistoryEntry } from "domain/entities/HistoryEntry";
import { BaseApi } from "../../infrastructure/Api/BaseApi";

const API_URL = "/api/history";

export class HistoryRemoteDataSource extends BaseApi {
    constructor() {
        super(API_URL);
    }

    async getAllUserHistories(): Promise<HistoryEntry[]> {
        return await this.get<HistoryEntry[]>("/");
    }

    async createOrUpdateUserHistory(questionId: string, roomId: string, attemptStartedAt: string, attemptCompletedAt: string, collaboratorId: string, attemptCode: string): Promise<void> {
        return await this.protectedPost<void>("/", { questionId, roomId, attemptStartedAt, attemptCompletedAt, collaboratorId, attemptCode });
    }

    async deleteSelectedHistories(selectedHistoryIds: string[]): Promise<void> {
        selectedHistoryIds.forEach(async (id) => {
            await this.delete<void>(`/${id}`);
        });
    }

    async deleteAllUserHistories(): Promise<void> {
        await this.delete<void>("/all");
    }
}

export const historyRemoteDataSource = new HistoryRemoteDataSource();