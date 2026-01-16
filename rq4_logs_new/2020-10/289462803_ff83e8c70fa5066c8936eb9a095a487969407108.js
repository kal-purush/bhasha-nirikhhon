import React, { useContext, useEffect } from "react";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import CardColumns from "react-bootstrap/CardColumns";
import Form from "react-bootstrap/Form";
import axios from "axios";
import { Link } from "react-router-dom";
import { BookingsContext } from "../../Context/BookingsContext";
import { AuthContext } from "../../Context/AuthContext";
import { useTranslation } from "react-i18next";
import moment from "moment";
import "./auth.css";
import { PaymentContext } from "../../Context/PaymentContext";
import StripeNew from "../Stripe/StripeNew";

const UserProfilePage = () => {
	const { t } = useTranslation();

	const {		
		setPayingRemainder,		
		setOutstandingPayment,
		handleShowStripe,
		thisBooking,
		setThisBooking
	} = useContext(PaymentContext);

	const {
		data,
		setData,
		loginUsername,
		setLoginUsername,
		loginPassword,
		setLoginPassword,
	} = useContext(AuthContext);

	const getUser = () => {
		axios({
			method: "GET",
			withCredentials: true,
			url:
				process.env.REACT_APP_LOCATION === "development"
					? "http://localhost:4000/api/user"
					: "https://bodensdorf-server.herokuapp.com/api/user",
		}).then((res) => {
			setData(res.data);
			console.log(res);
		});
	};

	useEffect(() => {
		getUser();
		setPayingRemainder(true);
	}, []);

	const { bookingsList } = useContext(BookingsContext);
	const filteredList = data
		? bookingsList.filter((booking) => booking.userID === data._id)
		: undefined;
	const sortedList = filteredList
		? filteredList.sort((a, b) => a.arriveEpoch < b.arriveEpoch)
		: undefined;

	const logit = () => {
		console.log(thisBooking);
	};

	const handleClick = (amountOwed, _id) => {
		setOutstandingPayment(amountOwed);
		setThisBooking(filteredList.filter(booking => booking._id === _id)[0])
		setThisBooking((PrevState) => {
			return { ...PrevState, ["amtOwed"]: PrevState.amtOwed - amountOwed };
		});
		handleShowStripe();
	};

	return (
		<div id="userProfilePage">			
			<StripeNew />
			<span id="userProfileTop">
			<h4 className="userProfileTitle" onClick={logit}>
				{t("user6a")}
			</h4>
			<Link to="/"><Button className="userProfileTitle">{t("user14")}</Button></Link>
			</span>
			{data ? (
				<Form inline id="userDataForm">
					<span className="userFormGroup">
						<Form.Label className="userLabel">{t("user2")}</Form.Label>
						<Form.Control
							readOnly="readonly"
							type="text"
							value={data ? data.fName : undefined}
						/>
					</span>
					<span className="userFormGroup">
						<Form.Label className="userLabel">{t("user3")}</Form.Label>
						<Form.Control
							readOnly="readonly"
							type="text"
							value={data ? data.lName : undefined}
						/>
					</span>
					<span className="userFormGroup">
						<Form.Label className="userLabel">{t("user5")}</Form.Label>
						<Form.Control
							readOnly="readonly"
							type="text"
							value={data ? data.email : undefined}
						/>
					</span>
					<span className="userFormGroup">
						<Form.Label className="userLabel">{t("user4")}</Form.Label>
						<Form.Control
							readOnly="readonly"
							type="text"
							value={data ? data.telNo : undefined}
						/>
					</span>
				</Form>
			) : undefined}
			<h4 className="userProfileTitle">{t("user6")}</h4>
			<CardColumns id="bookingCards">
				{sortedList
					? sortedList.map((booking) => {
							return (
								<Card body className="individualBooking">
									<Card.Title>
										{moment(booking.arriveStr, "DD/MM/YYYY").format(
											"DD.MM.YYYY"
										)}{" "}
										-{" "}
										{moment(booking.departStr, "DD/MM/YYYY").format(
											"DD.MM.YYYY"
										)}
										<br />
										<br />
										{booking.amtOwed === 0 ? undefined : (
											<p className="paymentDueWarning">
												{t("payment2")}{" "}
												{moment(
													moment(booking.arriveEpoch).valueOf() - 2592000000
												).format("DD.MM.YYYY")}
												!
											</p>
										)}
									</Card.Title>
									<Card.Text as="div">
										<p>
											{t("user9")}
											{booking.people}
										</p>
										<p>
											{t("user10")}
											{booking.totalPrice}€
										</p>
										<p>
											{t("user11")}
											{booking.amtPaid}€
										</p>
										<p>
											{t("user12")}
											{booking.amtOwed}€
										</p>
									</Card.Text>
									{booking.amtOwed === 0 ? (
										<Button
											variant="secondary"
											className="payButton"
											size="lg"
											disabled
										>
											{t("payment3")}
										</Button>
									) : (
										<Button
											variant="primary"
											className="payButton"
											size="lg"
											onClick={function () {
												handleClick(booking.amtOwed, booking._id);
											}}
										>
											{t("payment4")}
										</Button>
									)}
								</Card>
							);
					  })
					: undefined}
			</CardColumns>
		</div>
	);
};

export default UserProfilePage;