export const ADD_TODO = 'ADD_TODO';
let toDoNextId = 0;
export function addToDo(text){
    return {
        type: ADD_TODO,
        id: toDoNextId++,
        text
    }
} 