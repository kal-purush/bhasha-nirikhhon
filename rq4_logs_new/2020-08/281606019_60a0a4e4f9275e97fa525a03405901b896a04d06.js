export default function (state = [], action) {
  switch (action.type) {
    case "FETCH_MESSAGES":
      return {
        ...state,
        messages: action.payload,
      };
    case "ADD_MESSAGES":
      return {
        ...state,
        messages: [...state.messages.concat(action.payload)],
      };
    default:
      return state;
  }
}