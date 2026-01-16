import { useState } from "react";

const useSubTasks = (initialSubTasks = [""]) => {
  const [subtasks, setSubtasks] = useState(initialSubTasks);

  const handleSubtaskChange = (index: number, value: string) => {
    const newSubtasks = [...subtasks];
    newSubtasks[index] = value
    setSubtasks(newSubtasks);
  };

  const handleAddSubtask = () => {
    setSubtasks([...subtasks, ""]);
  };

  const handleRemoveSubtask = (index: number) => {
    const newSubtasks = [...subtasks];
    newSubtasks.splice(index, 1);
    setSubtasks(newSubtasks);
  };

  return {
    subtasks,
    setSubtasks,
    handleAddSubtask,
    handleSubtaskChange,
    handleRemoveSubtask,
  };
};

export default useSubTasks;