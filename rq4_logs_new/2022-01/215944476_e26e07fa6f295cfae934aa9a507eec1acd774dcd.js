import React from "react";
import ReactDOM from "react-dom";
import { Elements } from "@stripe/react-stripe-js";
import { loadStripe } from "@stripe/stripe-js";

import CheckoutForm from "./CheckoutForm";

import "./App.css";

const stripePromise = loadStripe(
  "pk_test_51IQlWZDzQBKP7hmYTDtbeDYwphFjZYs4Jr5cj3j93YEBvqpVBgLB8zmwJe3vgfWUCWV97h41HwiowQVLNasPBe7Z00J2bCFIId"
);

function App() {
  return (
    <div className="App">
      <Elements stripe={stripePromise}>
        <CheckoutForm />
      </Elements>
    </div>
  );
}

export default App;