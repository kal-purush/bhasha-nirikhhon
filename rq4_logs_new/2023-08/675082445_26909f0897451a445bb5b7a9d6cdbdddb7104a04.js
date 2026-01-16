import Hero from "./components/Hero";
import Inputs from "./components/Inputs";
import Nav from "./components/Nav";

function App() {
  return (
    <div className="mx-auto w-full h-screen pt-4 py-5 px-2 bg-gradient-to-b from-cyan-700 to-blue-700 shadow-xl">
      <Inputs />
      <Nav />
      <Hero />
    </div>
  );
}

export default App;