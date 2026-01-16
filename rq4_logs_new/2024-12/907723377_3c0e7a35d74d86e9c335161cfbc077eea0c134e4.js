```javascript
//This is a React component that uses the useReducer hook to manage state.
function MyComponent() {
  const [state, dispatch] = useReducer(reducer, initialState);

  const reducer = (state, action) => {
    switch (action.type) {
      case 'INCREMENT':
        return { ...state, count: state.count + 1 };
      case 'DECREMENT':
        return { ...state, count: state.count - 1 };
      default:
        throw new Error();
    }
  };

  return (
    <ul>
      {state.items.map((item, index) => (
        <li key={index}>{item}</li> // Added unique key prop
      ))}
    </ul>
  );
}
```