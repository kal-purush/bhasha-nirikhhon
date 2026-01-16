import React from 'react'


export class SBCurrentPrices extends React.Component{

    constructor(){
        super()
        this.state = {
            currentPrice: {}
        }
    }
    fetchCurrentPrice = () => {
        fetch(`https://api.iextrading.com/1.0/stock/${this.props.companySymbol}/chart/1d`)
        .then(res => res.json())
        .then(prices => prices.length ?
            this.setState({currentPrice: prices[prices.length - 1]}) : null)
    }

    componentDidMount(){
        this.fetchCurrentPrice()
        setInterval(this.fetchCurrentPrice, 300000);
    }

    render(){
        return(
            <div>
            <p>
           {this.state.currentPrice["close"]}
            </p>
            </div>

        )
    }

}