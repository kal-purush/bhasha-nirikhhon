import React from "react";
import { Dropdown } from "./lib";
import { states } from "./Data";

export default function App() {
  return <Dropdown label="State" arr={states} field="name" />;
}