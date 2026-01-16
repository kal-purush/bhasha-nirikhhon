import React, { Component } from 'react'
import './Conversor.css'

export default class Conversor extends Component {
  constructor(props){
    super(props);

    this.state = {
      moedaA_valor: "",
      moedaB_valor: 0
    }

    this.converter = this.converter.bind(this);
  }

  converter() {


    let de_ = `${this.props.moedaA}`;
    let para_ = `${this.props.moedaB}`;
    let url = `https://api.exchangeratesapi.io/latest?base=${de_}&symbols=${para_}`;


    fetch(url)
        .then(res => {
            return (res.json())
        })
        
        .then(json => {
            let obj = json.rates;
            let cotacao = Object.values(obj)[0];
            let moedaB_valor = (parseFloat(this.state.moedaA_valor * cotacao).toFixed(2));
            this.setState({ moedaB_valor })
        })
  }

  render() {
    return (
      <div className="conversor">
        <h2>{this.props.moedaA} para {this.props.moedaB}</h2>

        <input type="text" onChange={(event) => {
          this.setState(this.setState({
            moedaA_valor: event.target.value
          })
          );
        }
        }>
        </input>

        <input type="button" value="Converter" onClick={this.converter}></input>
        <h2>{this.state.moedaB_valor}</h2>
      </div>
    );
  }
}