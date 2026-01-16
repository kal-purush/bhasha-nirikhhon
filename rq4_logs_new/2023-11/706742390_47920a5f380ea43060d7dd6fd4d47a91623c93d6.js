import "./App.css";
import ButtonComp from "./components/ButtonComp";
import AccordionComp from "./components/AccordionComp";
import CardComp from "./components/Card";
import TableComp from "./components/Table";

function App() {
  return (
    <div className="App">
      <h2>Introduction to React With Material UI</h2>
      <TableComp />
      <ButtonComp />
      <AccordionComp />
      <CardComp />
    </div>
  );
}

export default App;