import { useReducer } from "react";

function reducer(state, action) {
  console.log(`state is ${state}`, `action is ${action}`);
  switch (action.type) {
    case "inc":
      return { ...state, count: state.count + state.step };
    case "dec":
      return { ...state, count: state.count - state.step };
    case "defineCount":
      return { ...state, count: action.payload };
    case "reset":
      return { state: 0, count: 0 };
    case "defineStep":
      return { ...state, count: action.payload, step: action.payload };
    default:
      throw new Error("unknowm action");
  }
}

function DateCounter() {
  const initialState = { count: 0, step: 1 };
  const [state, dispatchCount] = useReducer(reducer, initialState);
  const { count, step } = state;
  // const [count, setCount] = useState(0);
  // const [step, setStep] = useState(1);

  // This mutates the date object.
  const date = new Date("june 21 2027");
  date.setDate(date.getDate() + count);

  const dec = function () {
    dispatchCount({ type: "dec" });
    // setCount((count) => count - 1);
    // setCount((count) => count - step);
  };

  const inc = function () {
    dispatchCount({ type: "inc" });
    // setCount((count) => count + 1);
    // setCount((count) => count + step);
  };

  const defineCount = function (e) {
    dispatchCount({ type: "defineCount", payload: Number(e.target.value) });
  };

  const defineStep = function (e) {
    dispatchCount({ type: "defineStep", payload: Number(e.target.value) });
  };

  const reset = function () {
    dispatchCount({ type: "reset" });
  };

  return (
    <div className="counter">
      <div>
        <input
          type="range"
          min="0"
          max="10"
          value={step}
          onChange={defineStep}
        />
        <span>{step}</span>
      </div>

      <div>
        <button onClick={dec}>-</button>
        <input value={count} onChange={defineCount} />
        <button onClick={inc}>+</button>
      </div>

      <p>{date.toDateString()}</p>

      <div>
        <button onClick={reset}>Reset</button>
      </div>
    </div>
  );
}
export default DateCounter;