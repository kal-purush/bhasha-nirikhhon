import {Product} from "@/models/Product";
import { Order } from "@/models/Order";
import {sendFetch} from "@/service/sendFetch";

const fetchProducts = async () : Promise<Array<Product>> => {
    return await sendFetch('https://kanpla-code-challenge.up.railway.app/products');
};

const fetchOrders = async () : Promise<Array<Order>> => {
    return await sendFetch('https://kanpla-code-challenge.up.railway.app/orders');
};

const fetchCreateOrder = async (totalOrder: {total: number }) : Promise<Order> => {
    return await sendFetch('https://kanpla-code-challenge.up.railway.app/orders', 'POST', totalOrder);
};

const fetchPayOrder = async (payment: { order_id: string | null; amount: number}) : Promise<Order> => {
    return await sendFetch('https://kanpla-code-challenge.up.railway.app/payments', 'POST', payment);
}

const fetchChangeStatus = async (orderId: string, status: { status: string; }) => {
    return await sendFetch(`https://kanpla-code-challenge.up.railway.app/orders/${orderId}`, 'PATCH', status);
}

export { fetchProducts, fetchOrders, fetchCreateOrder, fetchPayOrder, fetchChangeStatus };