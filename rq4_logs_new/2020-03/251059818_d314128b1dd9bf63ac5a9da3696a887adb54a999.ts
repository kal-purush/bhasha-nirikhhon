interface S {
  firstName?: string;
  lastName?: string;
  phoneNumber?: string;
  meta?: string;
  error?: { [key: string]: string };
  success?: string;
}

interface A {
  type: "submit" | "clear" | "change";
  data: S;
}

export const initialState: S = {
  error: {},
  firstName: "",
  lastName: "",
  phoneNumber: "",
  meta: "",
  success: ""
};

export function BuyModalFormReducer(state: S, action: A) {
  switch (action.type) {
    case "submit":
      state = action.data;
      break;
    case "clear":
      state = initialState;
    case "change":
      state = Object.assign(state, action.data);
    default:
      break;
  }
  return state;
}