import './App.css';

import { Upper } from './upper/Upper.js';
import { Content } from "./content/Content.js"
import { Footer } from './footer/Footer.js';
import { ScrollProvider } from './ScrollContext';

function App() {
  return (
    <ScrollProvider>
    <div className="App">
      <Upper />
      <Content />
      <Footer />

    </div>
    </ScrollProvider>
  );
}

export default App;