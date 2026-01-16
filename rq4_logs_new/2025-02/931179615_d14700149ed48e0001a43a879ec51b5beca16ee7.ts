import { useAtom } from "jotai";
import {
  showListAtom,
  showListTitleAtom,
  listTitleAtom,
  tasksAtom,
} from "../states/toDoAtoms";

export const useTodo = () => {
  const [showList, setShowList] = useAtom(showListAtom);
  const [showListTitle, setShowListTitle] = useAtom(showListTitleAtom);
  const [listTitle, setListTitle] = useAtom(listTitleAtom);
  const [tasks, setTasks] = useAtom(tasksAtom);

  return {
    showList,
    setShowList,
    showListTitle,
    setShowListTitle,
    listTitle,
    setListTitle,
    tasks,
    setTasks,
  };
};