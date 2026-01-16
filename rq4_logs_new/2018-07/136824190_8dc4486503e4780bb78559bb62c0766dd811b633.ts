export interface Item {
  id: number;
  text: string;
  status: string;
}

export interface AddTodoProps {
  text: string;
  status: string;
}