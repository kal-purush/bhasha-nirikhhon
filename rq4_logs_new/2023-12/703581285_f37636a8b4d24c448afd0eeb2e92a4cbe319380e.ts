import { useState, useEffect, FC } from 'react'
import { useRecoilValue } from 'recoil'
import { Todo } from '@/features/todo/api/types/index'
import { useSetTodosForStatus } from '@/features/todo/hooks/useSetTodosForStatus'
import { useSetTodos } from '@/hooks/useSetTodos'
import { incompletedTodoAtom } from '@/recoil/atoms/incompletedTodoAtom'

type UseTodoManagementResult = {
  open: boolean;
  setOpen: React.Dispatch<React.SetStateAction<boolean>>;
  todosByOne: Todo[];
  todosByTwo: Todo[];
  todosByThree: Todo[];
  todosByFour: Todo[];
  openSidebar: () => void;
};

export const useTodoManagement = (): UseTodoManagementResult => {
  const [open, setOpen] = useState(false)

  const todos = useSetTodos()
  useSetTodosForStatus(todos)

  const incompletedTodos = useRecoilValue(incompletedTodoAtom)

  const filterTodosByZone = (zone: number) => incompletedTodos.filter((todo) => todo.zone === zone)

  const [todosByOne, setTodosByOne] = useState(filterTodosByZone(1))
  const [todosByTwo, setTodosByTwo] = useState(filterTodosByZone(2))
  const [todosByThree, setTodosByThree] = useState(filterTodosByZone(3))
  const [todosByFour, setTodosByFour] = useState(filterTodosByZone(4))

  useEffect(() => {
    setTodosByOne(filterTodosByZone(1))
    setTodosByTwo(filterTodosByZone(2))
    setTodosByThree(filterTodosByZone(3))
    setTodosByFour(filterTodosByZone(4))
  }, [incompletedTodos])

  const openSidebar = () => {
    setOpen(true)
  }

  return {
    open,
    setOpen,
    todosByOne,
    todosByTwo,
    todosByThree,
    todosByFour,
    openSidebar,
  }
}