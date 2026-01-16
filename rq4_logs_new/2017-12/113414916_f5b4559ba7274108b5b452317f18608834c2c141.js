import React, {Component} from 'react';

export default class Tarea extends Component {
  render(){
    return (
      <div >
      <input type="checkbox" checked={this.props.input} onChange={this.theChange.bind(this)} />
      <p onClick={this.textClick.bind(this)}>{this.props.texto}</p>
      <button onClick={this.props.onButtonClick}>x</button>
      </div >
    )}
  theChange(ev){
    this.props.onCheckClick(ev, ev.target.checked)}

  textClick(ev){
    this.props.onTextClick(ev, this.props.texto)}
}