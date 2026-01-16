import React from "react";
import './NewItems.css'
//import FlipMove from "react-flip-move";

const NewItems = ({ newitems }) => {
    //<li key={newitems.id}>{newitems.text}</li>

    return (
        <ul>
            {newitems.map((todo) => <li key={todo.id}>{todo.text}</li>)}
        </ul>
    )
}
export default NewItems;