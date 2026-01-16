import { useRecoilValue, useSetRecoilState, SetterOrUpdater } from "recoil";
import { Todo, todosAtom, todosNotDoneSelector } from "@/states/todos";

export default function useTodo() {
  // todosの値を取得
  const state: Todo[] = useRecoilValue(todosAtom);

  // todosを更新
  const update: SetterOrUpdater<Todo[]> = useSetRecoilState(todosAtom);

  // todoの概要文を取得
  const getOverview = (todo: Todo): string => {
    return `【TODO${todo.id}】${todo.title}（${todo.done ? "完了" : "未完了"}）`;
  };

  return {
    state,
    update,
    getOverview,
    todosNotDoneSelector,
  };
}