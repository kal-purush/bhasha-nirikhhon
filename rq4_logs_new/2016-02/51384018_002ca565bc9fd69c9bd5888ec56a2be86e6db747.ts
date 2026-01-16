import {Todo} from "./ToDoInterface";

interface State {
    todos :Todo[];
    currentFilter : string;
}
export {State}