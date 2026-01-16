import React, { Component } from 'react';
import './App.css';

class App extends Component {
  state = {
    displayNum: '0',
    firstNum: '0',
    secondNum: '',
    operatiom: '',
  }

  onDisplay = (num) => {
    if(this.state.displayNum === '0') {
      this.setState({ displayNum: num });
    }

    else {
      const display = this.state.displayNum;
      const newDisplay = display.concat(num);
      this.setState({ displayNum: newDisplay });
    }
  }

  onClear = () => {
    const display = this.state.displayNum;
    const newDisplay = display.slice(0,display.length - 1);
    this.setState({ displayNum: newDisplay });
  }

  onAllClear = () => {
    this.setState({ displayNum: '' });
  }

  onWaitToCalculate = (op) => {
    const display = this.state.displayNum;
    this.setState({ firstNum: display });
    this.setState({ operation: op });
    this.setState({ displayNum: '' });
  } 

  onCalculate = () => {
    const display = this.state.displayNum;
    let n1 = parseFloat(this.state.firstNum);
    const op =this.state.operation;
    const n2 = parseFloat(display);
    console.log(n1,op,n2);
    let n3;
    switch(op) {
      case "+":
        n3 = n1 + n2;
        break;
      case "-":
        n3 = n1 - n2;
        break;
      case "*":
        n3 = n1 * n2;
        break;
      case "/":
        n3 = n1 / n2;
        break;
      case "%":
        n3 = n1 % n2;
        break;        
    }
    console.log(n3);
    const newDisplay = n3.toString();
    console.log(newDisplay);
    this.setState({ displayNum: newDisplay });
  }

  onReverse = () => {
    const display = this.state.displayNum;
    const start = display.slice(0,1);

    if(start === '-') { 
      const newDisplay = display.slice(1,display.length);
      this.setState({ displayNum : newDisplay });
    }

    else if(display === '0') {
    }

    else {
      const newDisplay = '-' + display.slice(0,display.length);
      this.setState({ displayNum: newDisplay });
    }
  }

  render() {
    return (
      <div className="App">
      <div className = "display">{this.state.displayNum}</div>
      <div className = "btn" onClick = {() => this.onWaitToCalculate('+')}>+</div>
      <div className = "btn" onClick = {() => this.onWaitToCalculate('-')}>-</div>
      <div className = "btn" onClick = {() => this.onWaitToCalculate('*')}>*</div>
      <div className = "btn" onClick = {() => this.onWaitToCalculate('/')}>/</div>
      <div className = "btn" onClick = {this.onReverse}>+/-</div>
      <div className = "btn" onClick = {(this.onClear)}>C</div>
      <div className = "btn" onClick = {(this.onAllClear)}>AC</div>
      <div className = "btn" onClick = {() => this.onDisplay('0')}>0</div>
      <div className = "btn" onClick = {() => this.onDisplay('1')}>1</div>
      <div className = "btn" onClick = {() => this.onDisplay('2')}>2</div>
      <div className = "btn" onClick = {() => this.onDisplay('3')}>3</div>
      <div className = "btn" onClick = {() => this.onDisplay('4')}>4</div>
      <div className = "btn" onClick = {() => this.onDisplay('5')}>5</div>
      <div className = "btn" onClick = {() => this.onDisplay('6')}>6</div>
      <div className = "btn" onClick = {() => this.onDisplay('7')}>7</div>
      <div className = "btn" onClick = {() => this.onDisplay('8')}>8</div>
      <div className = "btn" onClick = {() => this.onDisplay('9')}>9</div>
      <div className = "btn" onClick = {() => this.onDisplay('.')}>.</div>
      <div className = "btn btn-equal" onClick = {(this.onCalculate)}>=</div>
      </div>
    );
  }
}

export default App;