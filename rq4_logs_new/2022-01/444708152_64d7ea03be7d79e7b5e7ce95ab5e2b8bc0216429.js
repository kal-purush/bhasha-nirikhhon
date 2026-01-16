import React from "react";
import { Outlet, useNavigate } from 'react-router-dom';

export default function Modal() {
	let navigate = useNavigate();

	return (
		<div className="" style={modalStyle.container}>
			<div style={modalStyle.shadow}>
				<span onClick={()=>{navigate(-1)}} style={modalStyle.close}>&times;</span>
				<div style={modalStyle.content}>
					<Outlet />
				</div>
			</div>
		</div>
	)
}

const modalStyle = {
	container: {
		position: 'fixed',
		top: '50%',
		left: '50%',
		maxWidth: '100%',
		maxHeight: '100%',
		transform: 'translate(-50%, -50%)',
	},
	shadow: {
		display: 'inline-block',
		backgroundColor: '#ffffff',
		boxShadow: '0px 0px 8px 2px rgba( 0, 0, 0, 0.2 )',
		textAlign: 'right',
	},
	close: {
		textAlign: 'center',
		color: '#333333',
		cursor: 'pointer',
		margin: '3px',
		font: 'bold 38px/35px Helvetica, Verdana, Tahoma',
	},
	content: {
		width: '500px',
	},
};