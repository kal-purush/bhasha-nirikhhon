import React, { Component } from "react";
import classes from "./ConfirmOrder.module.scss";
import { connect } from "react-redux";
import { firebase } from "../../firebase/firebase";
import moment from "moment";

class ConfirmOrder extends Component {
  state = {
    updateId: null
  };
  componentWillMount = () => {
    firebase
      .database()
      .ref()
      .child("orders")
      .child(`${this.props.location.state.CustomerId}`)
      .once("value", snap => {
        for (let user in snap.val()) {
          if (snap.val()[user].id === this.props.location.state.id) {
            // console.log("true", user);
            this.setState({ updateId: user });
          }
        }
      });
  };
  handleChange = e => {
    const data = this.props.location.state;
    const updatedData = { ...data, status: e.target.value };
    firebase
      .database()
      .ref()
      .child("orders")
      .child(`${this.props.location.state.CustomerId}`)
      .child(`${this.state.updateId}`)
      .update(updatedData);
  };
  render() {
    const data = this.props.location.state;
    // console.log({ ...data, status: "delivered" });
    const allVal = ["pending", "delivered"];
    const {
      name,
      CustomerInfo,
      items,
      orderTime,
      status,
      totalPrice
    } = this.props.location.state;
    console.log(name, CustomerInfo, items, orderTime, status, totalPrice);
    const info = Object.keys(CustomerInfo);
    const orderedItems = Object.keys(items);
    // console.log(CustomerInfo["name"]);
    return (
      <div className={classes.container}>
        <div className={classes.wrapper}>
          {info.map(i => (
            <h3>
              {i} : {CustomerInfo[i]}
            </h3>
          ))}

          {orderedItems.map(i => (
            <h3>
              {i} : {items[i]}
            </h3>
          ))}
          <h3>OrderTime: {moment(orderTime).fromNow()}</h3>
          <h3>Total Price: {totalPrice}</h3>
          <select onChange={this.handleChange}>
            <option selected>{status}</option>
            {allVal.map(
              name => name !== status && <option value={name}>{name}</option>
            )}
          </select>
        </div>
      </div>
    );
  }
}

const mapStateToProps = state => {
  return {
    orders: state.orderReducer
  };
};
export default connect(mapStateToProps)(ConfirmOrder);