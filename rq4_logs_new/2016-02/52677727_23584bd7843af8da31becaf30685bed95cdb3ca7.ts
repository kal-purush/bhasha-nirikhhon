class TodoActionTypes{
    static ADD_TODO = 'ADD_TODO';
    static TOGGLE_TODO_STATE = 'TOGGLE_TODO_STATE';
    static APPLY_FILTER = 'APPLY_FILTER';
}


class TodoFilterTypes{
    static ALL = 'ALL';
    static COMPLETED = 'COMPLETED';
    static PENDING = 'PENDING';
}

export {TodoActionTypes,TodoFilterTypes};