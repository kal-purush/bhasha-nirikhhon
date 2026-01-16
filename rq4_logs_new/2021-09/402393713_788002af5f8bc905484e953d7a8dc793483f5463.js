import "./App.css";
import Header from "./components/Header/Header";
import Card from "./components/Card/Card";

function App() {
  return (
    <>
      <Header />
      <div className="inner-container">
        <Card />
        <Card />
        <Card />
      </div>
    </>
  );
}

export default App;