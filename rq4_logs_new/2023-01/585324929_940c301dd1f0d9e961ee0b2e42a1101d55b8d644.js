import "./App.css";
import ArrEx1 from "./components/arrEx/arr1";
import ArrEx2 from "./components/arrEx/arr2";
import ArrEx3 from "./components/arrEx/arr3";
import FirstComponent from "./components/first/FirstComponent";
import FormComponent from "./components/formComponent/FormComponent";
import SecondComponent from "./components/second/SecondComponent";
import BasedComp from "./components/stateCompEx/BasedCompEx";

function App() {
  return (
    <div className="App">
      <p className="pp">Edit</p>
      {/* <FirstComponent />
      <SecondComponent />
      <BasedComp />
      <ArrEx1 str={"ddd-Ffff-ggg"} />
      <ArrEx2 arr={[12, 4, 5, 23, 68]} min={5} max={30} />
      <ArrEx3 arr={[12, 4, 5, 23, 68]} /> */}
      <FormComponent />
    </div>
  );
}

export default App;