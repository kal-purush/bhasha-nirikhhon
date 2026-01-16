import { supabase } from "@/config/supabase";
import { GroceryProduct } from "@/types/groceryInterface";
import { User } from "@/types/userInterface";
import { GroceryStore } from "../store/groceryStore";
import { Product } from "@/types/productInterface";

export const groceryRouter = {
    async getUserGrocery(username: string) {
        if (!username) {
            return "Username is required"
        }
        try {
            const { data, error } = await supabase.from("users").select().eq("username", username);
            if (error) {
                return error.message
            }

            const user: User = data[0];
            if (!user) {
                return "Can't find user grocery, please checking username or user is available"
            }

            const groceryList = user.grocery_list;
            if (groceryList) {
                GroceryStore.setState({ groceryList })
                return groceryList
            }
        } catch (error) {
            return "Internal server error"
        }
    },
    async addNewUserGrocery(username: string, groceryData: GroceryProduct) {
        if (!username) {
            return "Username or new grocery data is required"
        }
        try {
            const { error, data: selectedUser } = await supabase.from("users").select().eq("username", username)
            if (error) {
                return error.message
            }
            const user: User = selectedUser[0]
            const userGroceryList = user.grocery_list
            const userGrocery = userGroceryList ? [...userGroceryList, groceryData] : [groceryData]
            await supabase.from("users").update({ grocery_list: userGrocery }).eq("username", username)
            if (userGroceryList) {
                return this.getUserGrocery(username)
            }
        } catch (error) {
            return error
        }
    },
    async deleteUserGroceryItem(username: string, groceryItemId: number) {
        if (!username || !groceryItemId) {
            return "Username or Grocery Item Id required"
        }

        try {
            const { data, error } = await supabase
                .from('users')
                .select('grocery_list')
                .eq('username', username)
                .single();

            if (error) {
                throw new Error(error.message);
            }

            const user = data;
            const userGroceryList = user.grocery_list;

            if (!userGroceryList) {
                return "Grocery list not found"
            }

            const deletedGroceryProductIndex = userGroceryList.findIndex((product: Product) => parseInt(product.id.toString()) === groceryItemId);

            if (deletedGroceryProductIndex === -1) {
                return "Product ID not found"
            }

            userGroceryList.splice(deletedGroceryProductIndex, 1);

            const { error: updateError } = await supabase
                .from('users')
                .update({ grocery_list: userGroceryList })
                .eq('username', username);

            if (updateError) {
                throw updateError;
            }
            this.getUserGrocery(username)
            return { status: "Deleted grocery" }
        } catch (error) {
            return { error };
        }
    },
    async updateUserGroceryItem(username: string, newGroceryData: GroceryProduct) {
        if (!username) {
            return "Username required"
        }
        try {
            const { data: selectedUser, error } = await supabase.from("users").select().eq("username", username).single()
            if (error) {
                throw new Error(error.message)
            }
            const userGroceryList = selectedUser.grocery_list
            const updatedGroceryData = userGroceryList.map((product: Product) => {
                if (product.id === newGroceryData.id) {
                    return { ...product, ...newGroceryData }
                } else {
                    return product
                }
            })
            await supabase.from("users").update({ grocery_list: updatedGroceryData }).eq("username", username)
            this.getUserGrocery(username)
        } catch (error) {
            return { error }
        }
    }
}