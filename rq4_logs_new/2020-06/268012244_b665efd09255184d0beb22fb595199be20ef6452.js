import React, { useState, useEffect, useContext } from "react";
import { Context } from "../store/appContext";
import { Link } from "react-router-dom";
import PropTypes from "prop-types";

export const EditContact = props => {
	const { store, actions } = useContext(Context);

	return (
		<div className="container">
			<div>
				<h1 className="text-center mt-5">Edit contact</h1>
				<form>
					<div className="form-group">
						<label>Full Name</label>
						<input
							value={store.name}
							type="text"
							className="form-control"
							placeholder="Full Name"
							onChange={e => {
								actions.editName(e.target.value);
							}}
						/>
					</div>
					<div className="form-group">
						<label>Email</label>
						<input
							value={store.email}
							type="email"
							className="form-control"
							placeholder="Enter email"
							onChange={e => {
								actions.editEmail(e.target.value);
							}}
						/>
					</div>
					<div className="form-group">
						<label>Phone</label>
						<input
							value={store.phone}
							type="phone"
							className="form-control"
							placeholder="Enter phone"
							onChange={e => {
								actions.editPhone(e.target.value);
							}}
						/>
					</div>
					<div className="form-group">
						<label>Address</label>
						<input
							value={store.address}
							type="text"
							className="form-control"
							placeholder="Enter address"
							onChange={e => {
								actions.editAddress(e.target.value);
							}}
						/>
					</div>
					<Link className="mt-3 w-100 text-center" to="/">
						<button
							type="button"
							className="btn btn-primary form-control"
							onClick={() => {
								actions.putAgenda();
								actions.checkEdit();
							}}>
							save
						</button>
					</Link>
					<Link className="mt-3 w-100 text-center" to="/">
						or get back to contacts
					</Link>
				</form>
			</div>
		</div>
	);
};

EditContact.propTypes = {
	card: PropTypes.any
};