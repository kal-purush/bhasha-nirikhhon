import { Request, Method } from "./Request";
import { PaginatedTransfersV2 } from "../response/PaginatedTransfersV2";
declare class ListTransfersV2 implements Request<PaginatedTransfersV2> {
    readonly __r: PaginatedTransfersV2 | undefined;
    readonly method: Method;
    readonly path: string;
    readonly bodyParams: {
        shop_id?: string;
        shop_name?: string;
        customer_id?: string;
        customer_name?: string;
        transaction_id?: string;
        private_money_id?: string;
        is_modified?: boolean;
        transaction_types?: string[];
        next_page_cursor_id?: string;
        prev_page_cursor_id?: string;
        per_page?: number;
        transfer_types?: string[];
        description?: string;
        from?: string;
        to?: string;
    };
    constructor(params?: {
        shop_id?: string;
        shop_name?: string;
        customer_id?: string;
        customer_name?: string;
        transaction_id?: string;
        private_money_id?: string;
        is_modified?: boolean;
        transaction_types?: string[];
        next_page_cursor_id?: string;
        prev_page_cursor_id?: string;
        per_page?: number;
        transfer_types?: string[];
        description?: string;
        from?: string;
        to?: string;
    });
}
export { ListTransfersV2 };