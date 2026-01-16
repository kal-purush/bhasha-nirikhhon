
import './App.css';
import ErrorBoundary from './Components/ErrorBoundary';
import Hero from './Components/Hero';

function App() {
  return (
    <div className="App">
      <ErrorBoundary>
        <Hero heroName="Batman"/>
      </ErrorBoundary>

      <ErrorBoundary>
        <Hero heroName="Superman"/>
      </ErrorBoundary>
     
      <ErrorBoundary>
        <Hero heroName="joker"/>
      </ErrorBoundary>
     
      
      
    </div>
  );
}

export default App;