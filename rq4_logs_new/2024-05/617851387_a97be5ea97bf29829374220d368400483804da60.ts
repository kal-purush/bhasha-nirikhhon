import ApiResponse, { ApiResponseSingle } from '../props/ApiResponses';
import { useState } from 'react';
import ApiRequestService from './ApiRequestService';
import FundProviderChat from '../props/models/FundProviderChat';
import FundProviderChatMessage from '../props/models/FundProviderChatMessage';

export class FundProviderChatService<T = FundProviderChat> {
    /**
     * Url prefix
     *
     * @param data
     */
    public prefix = '/platform/organizations';

    /**
     * @param apiRequest
     */
    public constructor(protected apiRequest: ApiRequestService<T> = new ApiRequestService<T>()) {}

    public list(
        organization_id: number,
        fund_id: number,
        fund_provider_id: number,
        query: object = {},
    ): Promise<ApiResponse<T>> {
        return this.apiRequest.get(
            `${this.prefix}/${organization_id}/funds/${fund_id}/providers/${fund_provider_id}/chats`,
            query,
        );
    }

    public store(
        organization_id: number,
        fund_id: number,
        fund_provider_id: number,
        query: object = {},
    ): Promise<ApiResponseSingle<T>> {
        return this.apiRequest.post(
            `${this.prefix}/${organization_id}/funds/${fund_id}/providers/${fund_provider_id}/chats`,
            query,
        );
    }

    public listMessages(
        organization_id: number,
        fund_id: number,
        fund_provider_id: number,
        chat_id: number,
        query: object = {},
    ): Promise<ApiResponse<FundProviderChatMessage>> {
        return this.apiRequest.get(
            `${this.prefix}/${organization_id}/funds/${fund_id}/providers/${fund_provider_id}/chats/${chat_id}/messages`,
            query,
        );
    }

    public storeMessage(
        organization_id: number,
        fund_id: number,
        fund_provider_id: number,
        chat_id: number,
        query: object = {},
    ): Promise<ApiResponseSingle<FundProviderChatMessage>> {
        return this.apiRequest.post(
            `${this.prefix}/${organization_id}/funds/${fund_id}/providers/${fund_provider_id}/chats/${chat_id}/messages`,
            query,
        );
    }
}

export default function useFundProviderChatService(): FundProviderChatService {
    return useState(new FundProviderChatService())[0];
}