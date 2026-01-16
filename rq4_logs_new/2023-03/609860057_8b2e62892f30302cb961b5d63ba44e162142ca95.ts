import { useDrop } from "react-dnd";
import { Status, TaskType } from "../types";

interface DragItem {
  type: TaskType["column_id"];
  id: string;
  from: Status;
}

export function useColumnDrop(
  column: Status,
  handleDrop: (fromColumn: Status, taskId: TaskType["id"]) => void
) {
  const [{ isOver }, dropRef] = useDrop<DragItem, void, { isOver: boolean }>({
    accept: "task",
    drop: (dragItem) => {
      console.log({ dragItem, column });
      if (!dragItem || dragItem.from === column) {
        return;
      }

      console.log({ dragItem, column });
      handleDrop(dragItem.from, dragItem.id);
    },
    collect: (monitor) => ({
      isOver: monitor.isOver(),
    }),
  });

  return {
    isOver,
    dropRef,
  };
}