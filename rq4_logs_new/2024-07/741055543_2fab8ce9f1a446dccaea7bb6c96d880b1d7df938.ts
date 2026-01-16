import * as httpRequest from "../../../utils/httpRequest";

export const getOrderDetail = async (orderId: string) => {
    try {
        const res = await httpRequest.get(`/order/${orderId}/details`);
        return res;
    } catch (error) {
        return Promise.reject(error);
    }
};

export const getOrderStatusLine = async (orderId: string) => {
    try {
        const res = await httpRequest.get(`/order/${orderId}/status-line`);
        return res;
    } catch (error) {
        return Promise.reject(error);
    }
};

export const reviewProductOrder = async (orderItemId: string, star: number, content: string) => {
    try {
        const res = await httpRequest.post(`/review`, {
            orderItemId, star, content
        });
        return res;
    } catch (error) {
        return Promise.reject(error);
    }
};

export const getReviewOrderDetail = async (orderId: string) => {
    try {
        const res = await httpRequest.get(`/order/${orderId}/order-item`);
        return res;
    } catch (error) {
        return Promise.reject(error);
    }
};

export const updateTransaction = async (transactionId: string) => {
    try {
        const res = await httpRequest.patch(`/transaction/${transactionId}`);
        return res;
    } catch (error) {
        return Promise.reject(error);
    }
};

export const getBranchNearest = async (lat: number, lng: number) => {
    try {
        const res = await httpRequest.get(`/branch/active?lat=${lat}&lng=${lng}&page=1&size=999`);
        return res;
    } catch (error) {
        return Promise.reject(error);
    }
};

export const requestCancelOrder = async (orderId: string, note: string) => {
    try {
        const res = await httpRequest.patch(`/order/${orderId}/user/cancel`, {
            note
        });
        return res;
    } catch (error) {
        return Promise.reject(error);
    }
};

export const cancellationRequestOrder = async (orderId: string, note: string) => {
    try {
        const res = await httpRequest.post(`/order/${orderId}/cancellation-request`, {
            note
        });
        return res;
    } catch (error) {
        return Promise.reject(error);
    }
};