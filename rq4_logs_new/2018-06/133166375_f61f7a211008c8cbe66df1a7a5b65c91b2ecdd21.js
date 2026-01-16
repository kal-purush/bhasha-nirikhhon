import React, { Component } from 'react';

class LendingEdit extends Component {
	state = {
		customer: '',
		customerInfo: '',
		startDate: '',
		returnDate: '',
		lendType: '',
		itemName: '',
		itemSerial: ''

	}
	componentDidMount(){
		const { lending } = this.props;

		this.setState({
			customer: lending.customer,
			customerInfo: lending.customerInfo,
			startDate: lending.startDate,
			returnDate: lending.returnDate,
			lendType: lending.lendType,
			itemName: lending.item.name,
			itemSerial: lending.item.serial
		});
	}

	render(){
		const { lending } = this.props;
		const { 
			customer,
			customerInfo,
			startDate,
			returnDate,
			lendType,
			itemName,
			itemSerial
		} = this.state;

		return (
			<div className="Items__dialog__inputContainer">
				<label htmlFor='customer'>Asiakas</label>
				<input 
					value={customer} 
					onChange={this.onCustomerChange}
				/>
				<label htmlFor='customerInfo'>Asiakkaan yhteystiedot</label>
				<input 
					value={customerInfo}
					onChange={this.onCustomerInfoChange}
				/>
				<label htmlFor='startDate'>Lainauspäivä</label>
				<input 
					value={startDate}
					onChange={this.onStartDateChange}
				/>
				<label htmlFor='returnDate'>Palautuspäivä</label>
				<input 
					value={returnDate}
					onChange={this.onEndDateChange}
				/>
				<label htmlFor='lendType'>Lainauksen luonne</label>
				<input 
					value={lendType}
					onChange={this.onLendTypeChange}
				/>
				<label htmlFor='itemName'>Tuotteen nimi</label>
				<input 
					value={itemName}
					onChange={this.onItemNameChange}
				/>
				<label htmlFor='itemSerial'>Tuotteen sarjanumero</label>
				<input 
					value={itemSerial}
					onChange={this.onItemSerialChange}
				/>

				<button>Tallenna</button>
				<button onClick={this.props.cancelEdit}>Peruuta</button>
			</div>
		);
	}
}

export default LendingEdit;