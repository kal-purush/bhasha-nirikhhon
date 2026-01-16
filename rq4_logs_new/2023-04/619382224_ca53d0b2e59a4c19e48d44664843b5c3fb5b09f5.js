import { useState } from "react";
import uniqid from "uniqid";
function TaskForm(props){
    const { identifier, addComplex } = props;
    const [task,setTask] = useState({
        id: uniqid(),
        name: '',
    });
    const setActualTask = (e) =>{
        setTask({
            ...task,
            name: e.target.value,
        })
    }
    return(
        <form onSubmit={(e)=>{
            e.preventDefault();
            addComplex(identifier, task);
            setTask({
                id: uniqid(),
                name: '',
            })
        }}>
            <input type="text" id="task" onChange={setActualTask} value={task.name} placeholder="Add task you were doing"/>
            <button type="submit">+</button>
        </form>
    )
}
export default TaskForm;