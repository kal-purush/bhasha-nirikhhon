import { useState } from "react"

export default function AddNew(props) {

    const [todo, setTodo] = useState("");

    const newTodo = (e) => {
        setTodo(e.target.value);
    }

    const handleTodo = () => {
        props.addNew(todo);
        setTodo("");
    }

    const enterKey = (e)=>{
        e.key === "Enter" && handleTodo();
    }

    return <>
        <div className="addTodoContainer">
            <input type="text" onChange={newTodo} onKeyDown={enterKey} value={todo} placeholder="Enter Todo"></input>
            <button onClick={handleTodo}>Add Todo</button>
        </div>
    </>
}