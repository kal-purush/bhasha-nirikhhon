import React, { Component } from 'react';
//import logo from './logo.svg';
import '../Styles/style.css';

const Header = () => (
  <header className="App-header">
    {/* <img src={logo} className="App-logo" alt="logo" /> */}
    <h1 className="App-title">TiP Calculator</h1>
  </header>
)

const SVG = (props) => ( 
  <div className='svg'> 
    <svg width="352" height="660" viewBox="0 0 352 660" >
      <defs>
          <linearGradient x1="100%" y1="0%" x2="0%" y2="100%" id="linearGradient-1">
              <stop stopColor="#DD8EDA" offset="0%" />
              <stop stopColor="#42A3F9" offset="100%" />
          </linearGradient>
      </defs>
      <g id="Page-1" fill="none" fillRule="evenodd">
          <g id="Desktop-HD-Copy" transform="translate(-544 -219)" fill="url(#linearGradient-1)">
              <path d="M544,457.004454 L544,219 L896,219 L896,457.004454 C881.042868,457.271138 869,469.479188 869,484.5 C869,499.520812 881.042868,511.728862 896,511.995546 L896,879 L544,879 L544,511.995546 C544.166311,511.998511 544.332981,512 544.5,512 C559.687831,512 572,499.687831 572,484.5 C572,469.312169 559.687831,457 544.5,457 C544.332981,457 544.166311,457.001489 544,457.004454 Z"
              id="Combined-Shape-Copy" />
          </g>
      </g>
    </svg>      
  </div>
)


const Calculator = (props) => (
  <div className='display'>

    <div className="result">
      <div className="tipLabel">Tip</div>
      <div className="label">total</div>
      <div className="tip">$15.00</div>
      <div className="perPersonLabel">per person</div>
      <div className="perPerson">$7.50</div>
    </div>

    <form>
      <div className="label">Bill</div>
      <input type="number"/>
    </form>


    

      
  </div>
)






class TipCalculatorApp extends Component {
  render() {
    return (
      <div className="App">
         <Header />
         <SVG /> 
         <Calculator />
      </div>
    );

  }
}

export default TipCalculatorApp;