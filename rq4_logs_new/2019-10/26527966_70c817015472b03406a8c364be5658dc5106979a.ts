import { StateType, ActionType, Row } from "./types";

const initialState: StateType = {
  person: {
    name: "",
    address: "",
    postcode: "",
    town: "",
    tel: "",
    email: "",
    event: "",
    dept: "110 410",
    account: ""
  },
  rows: [],
  total: 0
};

const newRow = (id: number): Row => {
  return {
    id: id,
    date: new Date(),
    company: "",
    description: "",
    amount: 0
  };
};

const datePattern = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

function dateReviver(_key: string, value: string | number) {
  if (typeof value === "string" && datePattern.test(value)) {
    return new Date(value);
  }

  return value;
}

function retotal(state: StateType) {
  state.total = state.rows.reduce((acc, row) => {
    return acc + row.amount;
  }, 0);

  return state;
}

export function getInitialState() {
  const local = localStorage.getItem("itera-ekstern-utlegg");

  if (local === null) {
    return initialState;
  }

  return retotal(JSON.parse(local, dateReviver));
}

function updateStorage(state: StateType) {
  localStorage.setItem("itera-ekstern-utlegg", JSON.stringify(state));

  return state;
}

function validate(row: Row) {
  row.valid =
    row.amount > 0 &&
    row.company.length >= 2 &&
    row.description.length >= 3 &&
    row.date <= new Date();

  return row;
}

export function reducer(state: StateType, action: ActionType) {
  switch (action.type) {
    case "update_person":
      return updateStorage({
        ...state,
        person: action.person
      });
    case "clear_person":
      return updateStorage({
        ...state,
        person: initialState.person
      });
    case "add_row":
      const newRows = state.rows;
      newRows.push(newRow(state.rows.length));

      return updateStorage({
        ...state,
        rows: newRows
      });
    case "clear_rows":
      return updateStorage({
        ...state,
        rows: [],
        total: 0
      });
    case "update_row":
      return updateStorage(
        retotal({
          ...state,
          rows: state.rows.map(row =>
            row.id === action.row.id ? validate(action.row) : row
          )
        })
      );
    default:
      return state;
  }
}