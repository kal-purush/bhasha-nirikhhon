import React from 'react';

class Address extends React.Component {
  render() {
  	console.log('address');
  	console.log(this.props);
    var address = this.props.address;
    return (
      <div>
        <input name="address1" type="text" value={address.address1} onChange={this.handleChange} placeholder="Address 1" className="login-mail"/>
        <input name="address2" type="text" value={address.address2} onChange={this.handleChange} placeholder="Address 2" className="login-mail"/>
        <input name="city" type="text" value={address.city} onChange={this.handleChange} placeholder="City" className="login-mail"/>
        <input name="state" type="text" value={address.state} onChange={this.handleChange} placeholder="State" className="login-mail"/>
        <input name="zip" type="text" value={address.zip} onChange={this.handleChange} placeholder="Zip" className="login-mail"/>
        <input name="status" type="number" value={address.status} onChange={this.handleChange} placeholder="Status" className="login-mail"/>
        <input name="addressType" type="number" value={address.addressType} onChange={this.handleChange} placeholder="Address Type" className="login-mail"/>
      </div>
    )
  }

  handleChange = evt => {
    this.props.address[evt.target.name] = evt.target.value;
    this.setState(this.props.address);
  }
}

export default Address;