import { cart_item } from "@prisma/client";
import * as yup from "yup";

export interface INewCartItem {
    session_id: cart_item["session_id"];
    product_id: cart_item["product_id"];
    quantity: cart_item["quantity"];
}

export interface IUpdateCartItem {
    session_id?: cart_item["session_id"];
    product_id?: cart_item["product_id"];
    quantity?: cart_item["quantity"];
}

export const CartItemSchema = yup.object({
    session_id: yup
        .number()
        .min(0, "Invalid Session ID")
        .defined("Session ID is required"),
    product_id: yup
        .number()
        .min(0, "Invalid Product ID")
        .defined("Product ID is required"),
    quantity: yup
        .number()
        .min(0, "Invalid Quantity")
        .defined("Quantity is required"),
});

export const UpdateCartItemSchema = yup.object({
    session_id: yup.string().min(0, "Invalid Session ID").optional(),
    product_id: yup.number().min(0, "Invalid Product ID").optional(),
    quantity: yup.number().min(0, "Invalid Quantity").optional(),
});
