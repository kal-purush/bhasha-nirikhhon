import { React, useState } from "react";
import { useDispatch } from "react-redux";
import { addExpense } from "./expensesSlice";

const AddExpense = ({ budget_id }) => {
  const dispatch = useDispatch();

  const [inputs, setInputs] = useState({});

  const handleChange = (event) => {
    const name = event.target.name;
    const value = event.target.value;
    setInputs((values) => ({ ...values, [name]: value }));
  };

  const handleSubmit = (event) => {
    event.preventDefault();
    dispatch(
      addExpense({
        budget_id: budget_id,
        expense_name: inputs.expense_name,
        amount: inputs.amount,
      })
    );
  };

  return (
    <div>
      <br></br>
      <form onSubmit={handleSubmit}>
        <label>
          Name
          <input
            type="text"
            name="expense_name"
            value={inputs.expense_name || ""}
            onChange={handleChange}
          />
        </label>
        <label>
          Amount
          <input
            type="number"
            name="amount"
            value={inputs.amount || ""}
            onChange={handleChange}
          />
        </label>
        <input type="submit" />
      </form>
      <br></br>
    </div>
  );
};

export default AddExpense;