import { useState } from "react";
import { TaskProps, Task } from "../types/TaskProps";

export const useEdit = ({ tasks, setTasks }: TaskProps) => {
  const [editTaskId, setEditTaskId] = useState<number | null>(null);
  const [editedTask, setEditedTask] = useState<Partial<Task>>({});

  const handleEdit = (task: Task) => {
    setEditTaskId(task.id);
    setEditedTask({ ...task });
  };

  const handleChange = (field: keyof Task, value: any) => {
    setEditedTask((prev) => ({ ...prev, [field]: value }));
  };

  const handleUpdate = () => {
    const updatedTasks = tasks.map((task) =>
      task.id === editTaskId ? { ...task, ...editedTask } : task
    );
    setTasks(updatedTasks);

    setEditTaskId(null);
    setEditedTask({});
  };

  const handleCancel = () => {
    setEditTaskId(null);
    setEditedTask({});
  };

  return {
    editTaskId,
    editedTask,
    handleEdit,
    handleChange,
    handleUpdate,
    handleCancel,
  };
};