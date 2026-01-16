import { useState } from 'react';
import ApiRequestService from './ApiRequestService';
import PhysicalCard from '../props/models/PhysicalCard';
import {ApiResponseSingle} from "../props/ApiResponses";

export class PhysicalCardService<T = PhysicalCard> {
    /**
     * @param apiRequest
     */
    public constructor(protected apiRequest: ApiRequestService<T> = new ApiRequestService<T>()) {}

    /**
     * Url prefix
     *
     * @param data
     */
    public prefix = '/platform/sponsor';

    /**
     * Fetch by id
     */
    public read(organizationId: number, voucherId: number, data: object = {}): Promise<ApiResponseSingle<T>> {
        return this.apiRequest.get(`${this.prefix}/${organizationId}/vouchers/${voucherId}/physical-cards`, data);
    }

    /**
     * Store
     */
    public store(organizationId: number, voucherId: number, data: object = {}): Promise<ApiResponseSingle<T>> {
        return this.apiRequest.post(`${this.prefix}/${organizationId}/vouchers/${voucherId}/physical-cards`, data);
    }

    /**
     * Delete by id
     */
    public delete(organizationId: number, voucherId: number, id: number): Promise<null> {
        return this.apiRequest.delete(`${this.prefix}/${organizationId}/vouchers/${voucherId}/physical-cards/${id}`);
    }
}
export function usePhysicalCardService(): PhysicalCardService {
    return useState(new PhysicalCardService())[0];
}