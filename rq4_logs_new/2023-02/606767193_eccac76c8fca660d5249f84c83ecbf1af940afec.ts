import { GlobalContextProps, TransactionModel } from "./types";

type ACTIONTYPE =
    | { type: "DELETE_TRANSACTION"; payload: number }
    | { type: "ADD_TRANSACTION"; payload: TransactionModel };

export default (state: GlobalContextProps, action: ACTIONTYPE) => {
    switch (action.type) {
        case "ADD_TRANSACTION":
            return {
                ...state,
                transactions: [...state.transactions, action.payload],
            };
        case "DELETE_TRANSACTION":
            return {
                ...state,
                transactions: state.transactions.filter(
                    (item) => item.id !== action.payload
                ),
            };
        default:
            return state;
    }
};