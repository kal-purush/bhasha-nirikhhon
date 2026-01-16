import React from 'react'
import ShoppingCartActions from "./ShoppingCartActions";

class ProductForm extends React.Component{

    constructor(props) {
        super(props);
        this.handleNameChange = this.handleNameChange.bind(this);
        this.handlePriceChange = this.handlePriceChange.bind(this);
        this.handleSubmit = this.handleSubmit.bind(this);
        this.state = {name : '', price : 0.0}
    }

    handleNameChange(event){
        this.setState({name : event.target.value})
    }

    handlePriceChange(event){
        this.setState({price : event.target.value})
    }

    handleSubmit(event){
        ShoppingCartActions.insertProduct({
            name : this.state.name,
            price : this.state.price
        })
        this.setState({name : '', price : 0.0});
    }

    render(){
        return (
            <table>
                <tr>
                    <td>Name</td>
                    <td><input
                        value={this.state.name}
                        type='text'
                        onChange={this.handleNameChange}
                    /></td>
                </tr>
                <tr>
                    <td>Price</td>
                    <td><input
                        value={this.state.price}
                        type='number'
                        onChange={this.handlePriceChange}
                    /></td>
                </tr>
                <tr>
                    <td colSpan="2"><button onClick={this.handleSubmit}>Insert</button></td>
                </tr>
            </table>
        )
    }
}

export default ProductForm