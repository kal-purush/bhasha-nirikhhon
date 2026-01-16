import {Order} from "@/models/Order";
import {create} from "zustand/react";
import {createJSONStorage, persist} from "zustand/middleware";
import AsyncStorage from "@react-native-async-storage/async-storage";

interface IOrders {
    orders: Array<Order> | undefined;
    setOrders: (orders: Array<Order>) => void;
}

const initialState = {
    orders: []
};

const useOrderStore = create<IOrders>()(
    persist(
        (set) => ({
            ...initialState,
            setOrders: (orders: Array<Order>) => set(() => ({ orders })),
        }),
        {
            name: 'OrdersList',
            storage: createJSONStorage(() => AsyncStorage),
        }
    )
);

export { useOrderStore }