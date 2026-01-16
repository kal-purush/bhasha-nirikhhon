//actions
const ADD_EXPENSE = "ADD_EXPENSE";
const DELETE_EXPENSE = "DELETE_EXPENSE";
const SEARCH_EXPENSE = "SEARCH_EXPENSE";

//action creator
export function addExpense(data) {
    return { type: ADD_EXPENSE, payload: data };
}

export function deleteExpense(data) {
    return { type: DELETE_EXPENSE, payload: data };
}

export function searchExpense(data) {
    return { type: SEARCH_EXPENSE, payload: data };
}