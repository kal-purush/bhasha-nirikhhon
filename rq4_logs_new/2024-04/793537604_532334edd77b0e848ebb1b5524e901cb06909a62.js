import Expenses from "./components/expenses/Expenses";
import NewExpense from "../src/components/NewExpense/NewExpense";
import { useState } from "react";

function App() {
  const DUMMY_EXPENSE = [
    {
      id: "1",
      title: "Toilet Paper",
      amount: 100,
      date: new Date(2024, 0, 10),
    },
    {
      id: "2",
      title: "New TV",
      amount: 200,
      date: new Date(2024, 1, 10),
    },
    {
      id: "3",
      title: "New Fridge",
      amount: 300,
      date: new Date(2023, 2, 10),
    },
  ];
  const [expenses, setExpenses] = useState(DUMMY_EXPENSE);

  const addExpenseHandler = (expenseData) => {
    setExpenses((prevExpenses) => {
      return [expenseData, ...prevExpenses];
    });
  };
  return (
    <div className="App">
      <NewExpense onAddExpenseData={addExpenseHandler}></NewExpense>
      <Expenses items={expenses} />
    </div>
  );
}

export default App;