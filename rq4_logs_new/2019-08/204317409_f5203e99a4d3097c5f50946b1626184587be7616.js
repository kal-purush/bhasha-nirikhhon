import React, {Component} from "react";
import {connect} from "react-redux";
import ProductAdd from "./index"
import {editProduct, addProduct} from "../../store/products/actions"

class Container extends Component {
    render() {
        return <ProductAdd 
            list={this.props.list} 
            navigation={this.props.navigation}
            addProduct={this.props.addProduct} 
            />
    }  
}

const putStateToProps = state => { 
    return {
        list: state.products.list
    };
}

const putDispatchToProps = {
    addProduct
}

export default connect(putStateToProps, putDispatchToProps)(Container)