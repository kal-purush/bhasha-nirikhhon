import React, { Component } from "react";
import {
  UncontrolledDropdown,
  DropdownToggle,
  DropdownMenu,
  DropdownItem,
  Badge,
  NavItem,
  NavLink,
} from "reactstrap";

class CartSummary extends Component {
  renderSummery() {
    return (
        <UncontrolledDropdown nav inNavbar>
          <DropdownToggle nav caret>
            Your cart - {this.props.cart.length}
          </DropdownToggle>
          <DropdownMenu right>
            {this.props.cart.map((cartItem) => (
              <DropdownItem key={cartItem.product.id}>

                <Badge color="danger" 
                onClick={()=>this.props.removeFromCart(cartItem.product)}>x                   
                </Badge>

                {cartItem.product.productName}
                <Badge color="success">{cartItem.quantity}</Badge>
              </DropdownItem>
            ))}

            <DropdownItem divider />
            <DropdownItem>Reset</DropdownItem>
          </DropdownMenu>
        </UncontrolledDropdown>
      
    );
  }

  renderEmptyCart(){
    return(
        <NavItem>
            <NavLink>Empty Cart</NavLink>
        </NavItem>
    )
  }

  render() {
      
    return <div> {this.props.cart.length > 0 ? this.renderSummery() : this.renderEmptyCart()}</div>;
  }
}

export default CartSummary;