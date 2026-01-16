import { supabase } from "@/config/supabase";
import { GroceryProduct } from "@/types/groceryInterface";

export const orderListRouter = {
    addNewOrderList: async (orderData: GroceryProduct[]) => {
        try {
            const { status, error } = await supabase.from("order_list").insert({ orderData }).select()
            if (error) {
                return error.message
            }
            return status
        } catch (error) {
            console.error(error);
            return error
        }
    }
}